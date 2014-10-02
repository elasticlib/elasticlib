package store.server.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.config.Config;
import store.common.exception.RepositoryClosedException;
import store.common.exception.SelfReplicationException;
import store.common.exception.UnknownReplicationException;
import store.common.exception.UnknownRepositoryException;
import store.common.model.AgentInfo;
import store.common.model.AgentState;
import store.common.model.CommandResult;
import store.common.model.ContentInfoTree;
import store.common.model.Operation;
import store.common.model.ReplicationInfo;
import store.common.value.Value;
import store.server.Content;
import static store.server.TestUtil.LOREM_IPSUM;
import static store.server.TestUtil.async;
import static store.server.TestUtil.config;
import static store.server.TestUtil.recursiveDelete;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.manager.ManagerModule;
import store.server.repository.Repository;

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
    private Content content;

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
        Content previousContent = content;
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
        async(new Runnable() {
            @Override
            public void run() {
                assertReplicationStopped();
            }
        });
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
        async(new Runnable() {
            @Override
            public void run() {
                assertThat(replicationsService.listReplicationInfos()).isEmpty();
            }
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
        try {
            try (InputStream inputStream = content.getInputStream()) {
                CommandResult firstStepResult = repository.addContentInfo(content.getInfo());
                CommandResult secondStepResult = repository.addContent(firstStepResult.getTransactionId(),
                                                                       content.getInfo().getContent(),
                                                                       inputStream);

                assertThat(firstStepResult.getOperation()).isEqualTo(Operation.CREATE);
                assertThat(secondStepResult).isEqualTo(firstStepResult);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void updateSourceContent(String newValue) {
        content = content.add("test", Value.of(newValue));
        repositoriesService.getRepository(SOURCE).addContentInfo(content.getInfo());
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
        async(new Runnable() {
            @Override
            public void run() {
                AgentInfo info = replicationInfo().getAgentInfo();
                assertThat(info.getState()).isEqualTo(AgentState.WAITING);
                assertThat(info.getCurSeq()).isEqualTo(info.getMaxSeq());

                assertDestinationHas(content);
            }
        });
    }

    private void assertDestinationHas(Content content) {
        ContentInfoTree actual = repositoriesService.getRepository(DESTINATION).getContentInfoTree(content.getHash());
        assertThat(actual).isEqualTo(content.getTree());
    }
}
