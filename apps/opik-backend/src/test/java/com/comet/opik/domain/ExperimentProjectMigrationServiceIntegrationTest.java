package com.comet.opik.domain;

import com.comet.opik.api.Dataset;
import com.comet.opik.api.ExperimentItem;
import com.comet.opik.api.OpikVersion;
import com.comet.opik.api.Project;
import com.comet.opik.api.Trace;
import com.comet.opik.api.WorkspaceVersion;
import com.comet.opik.api.resources.utils.ClickHouseContainerUtils;
import com.comet.opik.api.resources.utils.MigrationUtils;
import com.comet.opik.api.resources.utils.MySQLContainerUtils;
import com.comet.opik.api.resources.utils.RedisContainerUtils;
import com.comet.opik.api.resources.utils.TestDropwizardAppExtensionUtils;
import com.comet.opik.api.resources.utils.TestDropwizardAppExtensionUtils.AppContextConfig;
import com.comet.opik.api.resources.utils.TestDropwizardAppExtensionUtils.CustomConfig;
import com.comet.opik.api.resources.utils.TestUtils;
import com.comet.opik.api.resources.utils.WireMockUtils;
import com.comet.opik.api.resources.utils.resources.DatasetResourceClient;
import com.comet.opik.api.resources.utils.resources.ExperimentResourceClient;
import com.comet.opik.api.resources.utils.resources.ProjectResourceClient;
import com.comet.opik.api.resources.utils.resources.TraceResourceClient;
import com.comet.opik.extensions.DropwizardAppExtensionProvider;
import com.comet.opik.extensions.RegisterApp;
import com.comet.opik.infrastructure.auth.RequestContext;
import com.comet.opik.podam.PodamFactoryUtils;
import com.comet.opik.utils.AsyncUtils;
import com.google.inject.Injector;
import com.redis.testcontainers.RedisContainer;
import jakarta.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.mysql.MySQLContainer;
import ru.vyarus.dropwizard.guice.test.ClientSupport;
import ru.vyarus.dropwizard.guice.test.jupiter.ext.TestDropwizardAppExtension;
import uk.co.jemos.podam.api.PodamFactory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.comet.opik.api.resources.utils.AuthTestUtils.mockTargetWorkspace;
import static com.comet.opik.api.resources.utils.ClickHouseContainerUtils.DATABASE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Black-box integration test for the V1→V2 experiment project migration job.
 *
 * <p>The migration runs as a recurring Quartz job (configured with short interval for tests).
 * Tests seed data via REST API, then wait for the job to process it.
 *
 * <p>Positive-path tests use {@code findEligibleExperimentWorkspaces} (which uses FINAL for
 * correct deduplication) to verify migration completion. Negative-path tests use the workspace
 * version REST endpoint to verify the workspace stays V1.
 *
 * <p>Note: the workspace version endpoint relies on {@code hasVersion1Experiments} which does
 * not use FINAL — unmerged ClickHouse rows delay V2 promotion. This is acceptable in production
 * (cached, eventual consistency) but makes it unreliable for positive-path test assertions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(DropwizardAppExtensionProvider.class)
class ExperimentProjectMigrationServiceIntegrationTest {

    private static final String EXCLUDED_WORKSPACE_ID_1 = UUID.randomUUID().toString();
    private static final String EXCLUDED_WORKSPACE_ID_2 = UUID.randomUUID().toString();

    private final RedisContainer REDIS = RedisContainerUtils.newRedisContainer();
    private final GenericContainer<?> ZOOKEEPER_CONTAINER = ClickHouseContainerUtils.newZookeeperContainer();
    private final ClickHouseContainer CLICKHOUSE_CONTAINER = ClickHouseContainerUtils
            .newClickHouseContainer(ZOOKEEPER_CONTAINER);
    private final MySQLContainer MYSQL = MySQLContainerUtils.newMySQLContainer();

    private final WireMockUtils.WireMockRuntime wireMock;

    @RegisterApp
    private final TestDropwizardAppExtension APP;

    {
        Startables.deepStart(REDIS, CLICKHOUSE_CONTAINER, MYSQL, ZOOKEEPER_CONTAINER).join();

        wireMock = WireMockUtils.startWireMock();

        var databaseAnalyticsFactory = ClickHouseContainerUtils
                .newDatabaseAnalyticsFactory(CLICKHOUSE_CONTAINER, DATABASE_NAME);

        MigrationUtils.runMysqlDbMigration(MYSQL);
        MigrationUtils.runClickhouseDbMigration(CLICKHOUSE_CONTAINER);

        APP = TestDropwizardAppExtensionUtils.newTestDropwizardAppExtension(
                AppContextConfig.builder()
                        .jdbcUrl(MYSQL.getJdbcUrl())
                        .databaseAnalyticsFactory(databaseAnalyticsFactory)
                        .runtimeInfo(wireMock.runtimeInfo())
                        .redisUrl(REDIS.getRedisURI())
                        .customConfigs(List.of(
                                new CustomConfig("experimentProjectMigration.enabled", "true"),
                                new CustomConfig("experimentProjectMigration.startupDelay", "0s"),
                                new CustomConfig("experimentProjectMigration.interval", "5s"),
                                new CustomConfig("experimentProjectMigration.excludedWorkspaceIds",
                                        "%s,%s".formatted(EXCLUDED_WORKSPACE_ID_1, EXCLUDED_WORKSPACE_ID_2)),
                                new CustomConfig("cacheManager.enabled", "false")))
                        .build());
    }

    private final PodamFactory factory = PodamFactoryUtils.newPodamFactory();

    private ExperimentProjectMigrationService migrationService;
    private ExperimentDAO experimentDAO;
    private IdGenerator idGenerator;
    private ClientSupport client;
    private String baseUrl;

    private ProjectResourceClient projectResourceClient;
    private DatasetResourceClient datasetResourceClient;
    private ExperimentResourceClient experimentResourceClient;
    private TraceResourceClient traceResourceClient;

    @BeforeAll
    void setUpAll(ClientSupport clientSupport, Injector injector) {
        this.client = clientSupport;
        this.baseUrl = TestUtils.getBaseUrl(clientSupport);

        migrationService = injector.getInstance(ExperimentProjectMigrationService.class);
        experimentDAO = injector.getInstance(ExperimentDAO.class);
        idGenerator = injector.getInstance(IdGenerator.class);

        projectResourceClient = new ProjectResourceClient(client, baseUrl, factory);
        datasetResourceClient = new DatasetResourceClient(client, baseUrl);
        experimentResourceClient = new ExperimentResourceClient(client, baseUrl, factory);
        traceResourceClient = new TraceResourceClient(client, baseUrl);
    }

    @Test
    void migrateOrphanExperimentWithCertainInference() {
        var apiKey = UUID.randomUUID().toString();
        var workspaceName = randomName("workspace");
        var workspaceId = UUID.randomUUID().toString();
        mockTargetWorkspace(wireMock.server(), apiKey, workspaceName, workspaceId, randomName("user"));

        seedCertainExperiment(apiKey, workspaceName);

        // The recurring job picks up the workspace and migrates the orphan experiment.
        // findEligibleExperimentWorkspaces uses FINAL for correct post-INSERT deduplication.
        awaitNoEligibleExperiments(workspaceId);
    }

    @Test
    void skipAmbiguousExperimentWhenTracesPointToMultipleProjects() {
        var apiKey = UUID.randomUUID().toString();
        var workspaceName = randomName("workspace");
        var workspaceId = UUID.randomUUID().toString();
        mockTargetWorkspace(wireMock.server(), apiKey, workspaceName, workspaceId, randomName("user"));

        var projectName1 = randomName("project");
        var projectName2 = randomName("project");
        var projectId1 = createProject(apiKey, workspaceName, projectName1);
        createProject(apiKey, workspaceName, projectName2);
        var datasetName = randomName("dataset");
        createDatasetWithProject(apiKey, workspaceName, datasetName, projectId1);
        var trace1Id = createTrace(apiKey, workspaceName, projectName1);
        var trace2Id = createTrace(apiKey, workspaceName, projectName2);
        var experimentId = createOrphanExperiment(apiKey, workspaceName, datasetName);

        experimentResourceClient.createExperimentItem(Set.of(
                ExperimentItem.builder()
                        .id(idGenerator.generateId())
                        .experimentId(experimentId)
                        .traceId(trace1Id)
                        .datasetItemId(idGenerator.generateId())
                        .build(),
                ExperimentItem.builder()
                        .id(idGenerator.generateId())
                        .experimentId(experimentId)
                        .traceId(trace2Id)
                        .datasetItemId(idGenerator.generateId())
                        .build()),
                apiKey, workspaceName);

        // Wait for at least one job cycle, then verify workspace stays V1
        TestUtils.waitForMillis(8000);
        assertWorkspaceVersion(apiKey, workspaceName, OpikVersion.VERSION_1);
    }

    @Test
    void skipExperimentWhenInferredProjectWasDeleted() {
        var apiKey = UUID.randomUUID().toString();
        var workspaceName = randomName("workspace");
        var workspaceId = UUID.randomUUID().toString();
        mockTargetWorkspace(wireMock.server(), apiKey, workspaceName, workspaceId, randomName("user"));

        var projectName = randomName("project");
        var projectId = createProject(apiKey, workspaceName, projectName);
        var datasetName = randomName("dataset");
        createDatasetWithProject(apiKey, workspaceName, datasetName, projectId);
        var traceId = createTrace(apiKey, workspaceName, projectName);
        var experimentId = createOrphanExperiment(apiKey, workspaceName, datasetName);
        linkExperimentToTrace(apiKey, workspaceName, experimentId, traceId);

        projectResourceClient.deleteProject(projectId, apiKey, workspaceName);

        TestUtils.waitForMillis(8000);
        assertWorkspaceVersion(apiKey, workspaceName, OpikVersion.VERSION_1);
    }

    @Test
    void idempotentAcrossMultipleCycles() {
        var apiKey = UUID.randomUUID().toString();
        var workspaceName = randomName("workspace");
        var workspaceId = UUID.randomUUID().toString();
        mockTargetWorkspace(wireMock.server(), apiKey, workspaceName, workspaceId, randomName("user"));

        seedCertainExperiment(apiKey, workspaceName);

        awaitNoEligibleExperiments(workspaceId);

        // After additional job cycles, workspace stays migrated
        TestUtils.waitForMillis(8000);
        var remaining = experimentDAO.findEligibleExperimentWorkspaces(List.of(), 100)
                .contextWrite(ctx -> AsyncUtils.setRequestContext(ctx, "", ""))
                .collectList().block();
        assertThat(remaining.stream().map(OrphanWorkspace::workspaceId)).doesNotContain(workspaceId);
    }

    @Test
    void skipExperimentWithNoTraceLinks() {
        var apiKey = UUID.randomUUID().toString();
        var workspaceName = randomName("workspace");
        var workspaceId = UUID.randomUUID().toString();
        mockTargetWorkspace(wireMock.server(), apiKey, workspaceName, workspaceId, randomName("user"));

        var projectName = randomName("project");
        var projectId = createProject(apiKey, workspaceName, projectName);
        var datasetName = randomName("dataset");
        createDatasetWithProject(apiKey, workspaceName, datasetName, projectId);
        createOrphanExperiment(apiKey, workspaceName, datasetName);

        TestUtils.waitForMillis(8000);
        assertWorkspaceVersion(apiKey, workspaceName, OpikVersion.VERSION_1);
    }

    @Test
    void skipExcludedWorkspaces() {
        var apiKey = UUID.randomUUID().toString();
        var workspaceName = randomName("workspace");
        mockTargetWorkspace(wireMock.server(), apiKey, workspaceName, EXCLUDED_WORKSPACE_ID_1, randomName("user"));

        seedCertainExperiment(apiKey, workspaceName);

        // Excluded workspace should never be processed
        TestUtils.waitForMillis(8000);
        assertWorkspaceVersion(apiKey, workspaceName, OpikVersion.VERSION_1);
    }

    // --- Data seeding ---

    private void seedCertainExperiment(String apiKey, String workspaceName) {
        var projectName = randomName("project");
        var projectId = createProject(apiKey, workspaceName, projectName);
        var datasetName = randomName("dataset");
        createDatasetWithProject(apiKey, workspaceName, datasetName, projectId);
        var traceId = createTrace(apiKey, workspaceName, projectName);
        var experimentId = createOrphanExperiment(apiKey, workspaceName, datasetName);
        linkExperimentToTrace(apiKey, workspaceName, experimentId, traceId);
    }

    // --- Helpers ---

    private static String randomName(String prefix) {
        return "%s-%s".formatted(prefix, RandomStringUtils.secure().nextAlphanumeric(32));
    }

    private UUID createProject(String apiKey, String workspaceName, String projectName) {
        return projectResourceClient.createProject(
                Project.builder().name(projectName).build(), apiKey, workspaceName);
    }

    private void createDatasetWithProject(String apiKey, String workspaceName, String datasetName, UUID projectId) {
        datasetResourceClient.createDataset(
                Dataset.builder().name(datasetName).projectId(projectId).build(),
                apiKey, workspaceName);
    }

    private UUID createTrace(String apiKey, String workspaceName, String projectName) {
        return traceResourceClient.createTrace(
                Trace.builder().projectName(projectName).startTime(Instant.now()).build(),
                apiKey, workspaceName);
    }

    private UUID createOrphanExperiment(String apiKey, String workspaceName, String datasetName) {
        return experimentResourceClient.create(
                experimentResourceClient.createPartialExperiment()
                        .id(null)
                        .datasetName(datasetName)
                        .build(),
                apiKey, workspaceName);
    }

    private void linkExperimentToTrace(String apiKey, String workspaceName,
            UUID experimentId, UUID traceId) {
        experimentResourceClient.createExperimentItem(Set.of(
                ExperimentItem.builder()
                        .id(idGenerator.generateId())
                        .experimentId(experimentId)
                        .traceId(traceId)
                        .datasetItemId(idGenerator.generateId())
                        .build()),
                apiKey, workspaceName);
    }

    private OpikVersion getWorkspaceVersion(String apiKey, String workspaceName) {
        try (var response = client.target("%s/v1/private/workspaces/versions".formatted(baseUrl))
                .request()
                .header(HttpHeaders.AUTHORIZATION, apiKey)
                .header(RequestContext.WORKSPACE_HEADER, workspaceName)
                .get()) {
            assertThat(response.getStatus()).isEqualTo(200);
            return response.readEntity(WorkspaceVersion.class).opikVersion();
        }
    }

    private void assertWorkspaceVersion(String apiKey, String workspaceName, OpikVersion expected) {
        assertThat(getWorkspaceVersion(apiKey, workspaceName)).isEqualTo(expected);
    }

    private void awaitNoEligibleExperiments(String workspaceId) {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
            migrationService.runMigrationCycle().block();
            var remaining = experimentDAO.findEligibleExperimentWorkspaces(List.of(), 100)
                    .contextWrite(ctx -> AsyncUtils.setRequestContext(ctx, "", ""))
                    .collectList().block();
            assertThat(remaining.stream().map(OrphanWorkspace::workspaceId)).doesNotContain(workspaceId);
        });
    }
}
