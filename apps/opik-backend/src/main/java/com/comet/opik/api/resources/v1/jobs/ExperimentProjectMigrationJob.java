package com.comet.opik.api.resources.v1.jobs;

import com.comet.opik.domain.ExperimentProjectMigrationService;
import com.comet.opik.infrastructure.OpikConfiguration;
import com.comet.opik.infrastructure.lock.LockService;
import io.dropwizard.jobs.Job;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.comet.opik.infrastructure.lock.LockService.Lock;

@Singleton
@Slf4j
@DisallowConcurrentExecution
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class ExperimentProjectMigrationJob extends Job implements InterruptableJob {

    private static final Lock JOB_LOCK = new Lock("opik_job", ExperimentProjectMigrationJob.class.getSimpleName());

    private final @NonNull ExperimentProjectMigrationService migrationService;
    private final @NonNull OpikConfiguration opikConfiguration;
    private final @NonNull LockService lockService;

    private final AtomicBoolean interrupted = new AtomicBoolean(false);
    private final AtomicReference<Disposable> currentExecution = new AtomicReference<>();

    @Override
    public void doJob(JobExecutionContext context) {
        var config = opikConfiguration.getExperimentProjectMigration();
        if (!config.isEnabled()) {
            log.debug("Experiment project migration job is disabled, skipping");
            return;
        }
        if (interrupted.get()) {
            log.info("Experiment project migration job was interrupted before execution, skipping");
            return;
        }
        log.info("Starting experiment project migration job");
        var subscription = lockService.bestEffortLock(
                JOB_LOCK,
                Mono.defer(() -> {
                    if (interrupted.get()) {
                        log.info("Experiment project migration was interrupted before processing, skipping");
                        return Mono.empty();
                    }
                    return migrationService.runMigrationCycle().timeout(config.getJobTimeout().toJavaDuration());
                }),
                Mono.defer(() -> {
                    log.info("Could not acquire lock, another instance is already running");
                    return Mono.empty();
                }),
                config.getJobTimeout().toJavaDuration(),
                config.getLockTimeout().toJavaDuration(),
                true)
                .doOnError(throwable -> {
                    if (interrupted.get()) {
                        log.warn("Experiment project migration was interrupted", throwable);
                    } else {
                        log.error("Experiment project migration failed", throwable);
                    }
                })
                .doFinally(signal -> {
                    currentExecution.set(null);
                    if (signal == SignalType.ON_COMPLETE) {
                        log.info("Experiment project migration cycle completed");
                    }
                })
                .onErrorComplete()
                .subscribe();
        currentExecution.set(subscription);
    }

    @Override
    public void interrupt() {
        log.info("Interrupting experiment project migration job");
        interrupted.set(true);
        var execution = currentExecution.get();
        if (execution != null && !execution.isDisposed()) {
            execution.dispose();
            log.info("Experiment project migration job interrupted successfully");
        }
    }
}
