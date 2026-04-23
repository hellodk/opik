package com.comet.opik.domain;

import com.comet.opik.api.Project;
import com.comet.opik.domain.experiments.aggregations.ExperimentAggregationPublisher;
import com.comet.opik.infrastructure.OpikConfiguration;
import com.comet.opik.infrastructure.cache.CacheManager;
import com.google.common.collect.Lists;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.comet.opik.infrastructure.auth.RequestContext.SYSTEM_USER;
import static com.comet.opik.utils.AsyncUtils.setRequestContext;

@Slf4j
@Singleton
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class ExperimentProjectMigrationService {

    private static final String WORKSPACE_VERSION_CACHE_KEY_FORMAT = "opik:workspace_version:%s";

    private final @NonNull ExperimentDAO experimentDAO;
    private final @NonNull ProjectService projectService;
    private final @NonNull ExperimentAggregationPublisher experimentAggregationPublisher;
    private final @NonNull CacheManager cacheManager;
    private final @NonNull OpikConfiguration opikConfiguration;

    public Mono<Void> runMigrationCycle() {
        var config = opikConfiguration.getExperimentProjectMigration();
        log.info("Starting experiment project migration cycle, workspacesPerRun='{}', batchSize='{}'",
                config.getWorkspacesPerRun(), config.getExperimentBatchSize());
        return experimentDAO.findEligibleExperimentWorkspaces(
                config.getExcludedWorkspaceIds(), config.getWorkspacesPerRun())
                .contextWrite(ctx -> setRequestContext(ctx, SYSTEM_USER, ""))
                .collectList()
                .flatMap(orphanWorkspaces -> {
                    if (CollectionUtils.isEmpty(orphanWorkspaces)) {
                        log.info("No workspaces with eligible experiments found, consider disabling the job");
                        return Mono.empty();
                    }
                    log.info("Found workspaces with eligible experiments, count='{}'", orphanWorkspaces.size());
                    return Flux.fromIterable(orphanWorkspaces)
                            .concatMap(workspace -> migrateWorkspace(
                                    workspace.workspaceId(),
                                    workspace.orphanCount(),
                                    config.getExperimentBatchSize()))
                            .then();
                });
    }

    private Mono<Void> migrateWorkspace(String workspaceId, long eligibleCount, int batchSize) {
        log.info("Starting workspace migration, workspaceId='{}', eligibleCount='{}'", workspaceId, eligibleCount);
        return experimentDAO.computeExperimentProjectMapping()
                .contextWrite(ctx -> setRequestContext(ctx, SYSTEM_USER, workspaceId))
                .collectList()
                .flatMap(mappings -> {
                    if (CollectionUtils.isEmpty(mappings)) {
                        log.info("No certain experiments to migrate, workspaceId='{}'", workspaceId);
                        return Mono.empty();
                    }
                    var inferredProjectIds = mappings.stream()
                            .map(ExperimentProjectMapping::projectId)
                            .collect(Collectors.toSet());
                    return Mono.fromCallable(
                            () -> projectService.findByIds(workspaceId, inferredProjectIds))
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(existingProjects -> {
                                var validProjectIds = existingProjects.stream()
                                        .map(Project::id)
                                        .collect(Collectors.toSet());
                                var validated = mappings.stream()
                                        .filter(mapping -> validProjectIds.contains(mapping.projectId()))
                                        .toList();
                                var skippedDeleted = mappings.size() - validated.size();
                                if (skippedDeleted > 0) {
                                    log.info("Skipping experiments with deleted project, workspaceId='{}', count='{}'",
                                            workspaceId, skippedDeleted);
                                }
                                if (CollectionUtils.isEmpty(validated)) {
                                    log.info("No validated experiments to migrate, workspaceId='{}'", workspaceId);
                                    return Mono.empty();
                                }
                                var byProject = validated.stream()
                                        .collect(Collectors.groupingBy(ExperimentProjectMapping::projectId));
                                return Flux.fromIterable(byProject.entrySet())
                                        .concatMap(entry -> batchUpdateProjectId(
                                                workspaceId, entry.getKey(), entry.getValue(), batchSize))
                                        .then()
                                        .then(triggerReaggregation(workspaceId, validated))
                                        .then(evictWorkspaceVersionCache(workspaceId))
                                        .then(Mono.<Void>fromRunnable(() -> log.info(
                                                "Workspace migration completed, workspaceId='{}', migrated='{}', skippedDeletedProject='{}'",
                                                workspaceId, validated.size(), skippedDeleted)));
                            });
                })
                .onErrorResume(throwable -> {
                    log.error("Workspace migration failed, will retry next cycle, workspaceId='{}'",
                            workspaceId, throwable);
                    return Mono.empty();
                });
    }

    private Mono<Void> batchUpdateProjectId(
            String workspaceId, UUID projectId, List<ExperimentProjectMapping> experiments, int batchSize) {
        return Flux.fromIterable(Lists.partition(experiments, batchSize))
                .concatMap(batch -> {
                    var experimentIds = batch.stream()
                            .map(ExperimentProjectMapping::experimentId)
                            .collect(Collectors.toSet());
                    log.debug("Updating experiment batch, workspaceId='{}', projectId='{}', count='{}'",
                            workspaceId, projectId, experimentIds.size());
                    return experimentDAO.batchSetProjectId(experimentIds, projectId)
                            .contextWrite(ctx -> setRequestContext(ctx, SYSTEM_USER, workspaceId));
                })
                .then();
    }

    private Mono<Void> triggerReaggregation(String workspaceId, List<ExperimentProjectMapping> migrated) {
        var experimentIds = migrated.stream()
                .map(ExperimentProjectMapping::experimentId)
                .collect(Collectors.toSet());
        return experimentAggregationPublisher.publish(experimentIds, workspaceId, SYSTEM_USER);
    }

    private Mono<Void> evictWorkspaceVersionCache(String workspaceId) {
        var cacheKey = WORKSPACE_VERSION_CACHE_KEY_FORMAT.formatted(workspaceId);
        return cacheManager.evict(cacheKey, false)
                .onErrorResume(error -> {
                    log.warn("Failed to evict workspace version cache, workspaceId='{}'", workspaceId, error);
                    return Mono.just(false);
                })
                .then();
    }
}
