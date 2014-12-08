package org.elasticlib.node.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.exception.SelfReplicationException;
import org.elasticlib.common.exception.UnknownReplicationException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.Operation;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.value.Value;
import org.elasticlib.node.TestContent;
import static org.elasticlib.node.TestUtil.LOREM_IPSUM;
import static org.elasticlib.node.TestUtil.async;
import static org.elasticlib.node.TestUtil.config;
import static org.elasticlib.node.TestUtil.recursiveDelete;
import org.elasticlib.node.dao.ReplicationsDao;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.repository.Repository;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Replications service integration tests.
 */
public class ReplicationsServiceTest {

    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private static final String OTHER = "other";

    private Path path;
    private ManagerModule managerModule;
    private RepositoriesService repositoriesService;
    private ReplicationsService replicationsService;
    private TestContent content;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        Config config = config();

        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        managerModule = new ManagerModule(path.resolve("home"), config);

        RepositoriesDao repositoriesDao = new RepositoriesDao(managerModule.getStorageManager());
        ReplicationsDao replicationsDao = new ReplicationsDao(managerModule.getStorageManager());

        repositoriesService = new RepositoriesService(config,
                                                      managerModule.getTaskManager(),
                                                      managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      repositoriesDao);

        replicationsService = new ReplicationsService(managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      repositoriesDao,
                                                      replicationsDao,
                                                      repositoriesService);
        managerModule.start();
        repositoriesService.start();
        replicationsService.start();

        repositoriesService.createRepository(path.resolve(SOURCE));
        repositoriesService.createRepository(path.resolve(DESTINATION));
        repositoriesService.createRepository(path.resolve(OTHER));

        content = LOREM_IPSUM;
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        replicationsService.stop();
        repositoriesService.stop();
        managerModule.stop();
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test
    public void createReplicationTest() {
        addSourceContent();
        replicationsService.createReplication(SOURCE, DESTINATION);

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = SelfReplicationException.class)
    public void createReplicationSelfTest() {
        replicationsService.createReplication(SOURCE, SOURCE);
    }

    /**
     * Test.
     */
    @Test(expectedExceptions = UnknownRepositoryException.class)
    public void createReplicationUnknownRepositoryTest() {
        replicationsService.createReplication(SOURCE, "unknown");
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "createReplicationTest")
    public void createReplicationIsIdempotentTest() {
        replicationsService.createReplication(SOURCE, DESTINATION);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "createReplicationTest")
    public void newRepositoryEventTest() {
        updateSourceContent("newRepositoryEventTest");
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "newRepositoryEventTest")
    public void stopReplicationTest() {
        replicationsService.stopReplication(SOURCE, DESTINATION);
        TestContent previousContent = content;
        updateSourceContent("stopReplicationTest");

        assertReplicationStopped();
        assertDestinationHas(previousContent);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "stopReplicationTest")
    public void startReplicationTest() {
        replicationsService.startReplication(SOURCE, DESTINATION);

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "stopReplicationTest", expectedExceptions = UnknownReplicationException.class)
    public void startReplicationUnknownTest() {
        replicationsService.startReplication(SOURCE, OTHER);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "startReplicationTest")
    public void closeRepositoryTest() {
        repositoriesService.closeRepository(DESTINATION);
        async(this::assertReplicationStopped);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "closeRepositoryTest", expectedExceptions = RepositoryClosedException.class)
    public void startReplicationWithClosedRepositoryTest() {
        replicationsService.startReplication(SOURCE, DESTINATION);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "startReplicationWithClosedRepositoryTest")
    public void openRepositoryTest() {
        repositoriesService.openRepository(DESTINATION);
        updateSourceContent("openRepositoryTest");

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "openRepositoryTest")
    public void removeSourceRepositoryTest() {
        removeRepositoryTest(SOURCE);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "removeSourceRepositoryTest")
    public void removeDestinationRepositoryTest() {
        removeRepositoryTest(DESTINATION);
    }

    private void removeRepositoryTest(String repositoryName) {
        repositoriesService.removeRepository(repositoryName);
        async(() -> {
            assertThat(replicationsService.listReplicationInfos()).isEmpty();
        });

        // Restore previous state.
        repositoriesService.addRepository(path.resolve(repositoryName));
        replicationsService.createReplication(SOURCE, DESTINATION);

        assertReplicationStarted();
        assertDestinationUpToDate();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "removeDestinationRepositoryTest")
    public void deleteReplicationTest() {
        replicationsService.deleteReplication(SOURCE, DESTINATION);
        assertThat(replicationsService.listReplicationInfos()).isEmpty();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "deleteReplicationTest")
    public void deleteReplicationIsIdempotentTest() {
        replicationsService.deleteReplication(SOURCE, DESTINATION);
    }

    private ReplicationInfo replicationInfo() {
        List<ReplicationInfo> infos = replicationsService.listReplicationInfos();
        assertThat(infos).hasSize(1);

        return infos.get(0);
    }

    private void addSourceContent() {
        Repository repository = repositoriesService.getRepository(SOURCE);

        try (InputStream inputStream = content.getInputStream()) {
            StagingInfo stagingInfo = repository.stageContent(content.getHash());
            repository.writeContent(content.getHash(), stagingInfo.getSessionId(), inputStream, 0);

        } catch (IOException e) {
            throw new AssertionError(e);
        }
        CommandResult result = repository.addRevision(content.getRevision());
        assertThat(result.getOperation()).isEqualTo(Operation.CREATE);
    }

    private void updateSourceContent(String newValue) {
        content = content.add("test", Value.of(newValue));
        repositoriesService.getRepository(SOURCE).addRevision(content.getRevision());
    }

    private void assertReplicationStarted() {
        assertReplicationStarted(true);
    }

    private void assertReplicationStopped() {
        assertReplicationStarted(false);
    }

    private void assertReplicationStarted(boolean expected) {
        ReplicationInfo info = replicationInfo();

        assertThat(info.isStarted()).isEqualTo(expected);
        assertThat(info.getSourceDef().getName()).isEqualTo(SOURCE);
        assertThat(info.getDestinationdef().getName()).isEqualTo(DESTINATION);
    }

    private void assertDestinationUpToDate() {
        async(() -> {
            AgentInfo info = replicationInfo().getAgentInfo();
            assertThat(info.getState()).isEqualTo(AgentState.WAITING);
            assertThat(info.getCurSeq()).isEqualTo(info.getMaxSeq());

            assertDestinationHas(content);
        });
    }

    private void assertDestinationHas(TestContent content) {
        RevisionTree actual = repositoriesService.getRepository(DESTINATION).getTree(content.getHash());
        assertThat(actual).isEqualTo(content.getTree());
    }
}