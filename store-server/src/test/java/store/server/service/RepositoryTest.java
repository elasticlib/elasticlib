package store.server.service;

import com.google.common.collect.ImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.AgentInfo;
import store.common.AgentState;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.Event;
import store.common.IndexEntry;
import static store.common.IoUtil.copy;
import store.common.Operation;
import store.common.ReplicationInfo;
import store.common.RepositoryDef;
import store.common.RepositoryInfo;
import store.common.RepositoryStats;
import store.common.config.Config;
import store.common.hash.Hash;
import static store.common.metadata.Properties.Common.FILE_NAME;
import store.server.Content;
import static store.server.TestUtil.LOREM_IPSUM;
import static store.server.TestUtil.UNKNOWN_HASH;
import static store.server.TestUtil.recursiveDelete;
import store.server.config.ServerConfig;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_ENABLED;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_CLEANUP_INTERVAL;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_MAX_SIZE;
import static store.server.config.ServerConfig.STORAGE_SUSPENDED_TXN_TIMEOUT;
import static store.server.config.ServerConfig.STORAGE_SYNC_ENABLED;
import static store.server.config.ServerConfig.STORAGE_SYNC_INTERVAL;
import static store.server.config.ServerConfig.TASKS_POOL_SIZE;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.exception.ConflictException;
import store.server.exception.RepositoryClosedException;
import store.server.exception.UnknownContentException;
import store.server.manager.ManagerModule;
import store.server.repository.Repository;

/**
 * Unit tests.
 */
public class RepositoryTest {

    private static final String INIT = "init";
    private static final String OPERATIONS = "operations";
    private static final String DELETE = "delete";
    private static final String ALPHA = "alpha";
    private static final String BETA = "beta";
    private Path path;
    private ManagerModule managerModule;
    private RepositoriesService repositoriesService;
    private Map<String, RepositoryDef> defs;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        Config config = new Config()
                .set(TASKS_POOL_SIZE, 1)
                .set(STORAGE_SYNC_ENABLED, true)
                .set(STORAGE_SYNC_INTERVAL, "10 s")
                .set(STORAGE_SUSPENDED_TXN_MAX_SIZE, 10)
                .set(STORAGE_SUSPENDED_TXN_TIMEOUT, "10 s")
                .set(STORAGE_SUSPENDED_TXN_CLEANUP_ENABLED, true)
                .set(STORAGE_SUSPENDED_TXN_CLEANUP_INTERVAL, "10 s")
                .set(ServerConfig.JE_LOCK_TIMEOUT, "1 min");

        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        managerModule = new ManagerModule(path.resolve("home"), config);
        repositoriesService = new RepositoriesService(config,
                                                      managerModule.getTaskManager(),
                                                      managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      new RepositoriesDao(managerModule.getStorageManager()),
                                                      new ReplicationsDao(managerModule.getStorageManager()));

        managerModule.start();
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        repositoriesService.stop();
        managerModule.stop();
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test(groups = INIT)
    public void createRepositoryTest() {
        Path alphaPath = path.resolve(ALPHA);
        Path betaPath = path.resolve(BETA);

        repositoriesService.createRepository(alphaPath);
        repositoriesService.createRepository(betaPath);

        List<RepositoryInfo> infos = repositoriesService.listRepositoryInfos();
        assertThat(infos).hasSize(2);
        RepositoryDef alphaDef = infos.get(0).getDef();
        RepositoryDef betaDef = infos.get(1).getDef();

        check(alphaDef, ALPHA, alphaPath);
        check(betaDef, BETA, betaPath);

        defs = ImmutableMap.of(ALPHA, alphaDef,
                               BETA, betaDef);

        assertThat(repository(ALPHA).history(true, 0, 10)).isEmpty();
        assertThat(repository(BETA).history(true, 0, 10)).isEmpty();
    }

    private static void check(RepositoryDef def, String name, Path path) {
        assertThat(def.getName()).isEqualTo(name);
        assertThat(def.getPath()).isEqualTo(path);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = INIT, dependsOnMethods = "createRepositoryTest")
    public void addTest() throws IOException {
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            Repository alpha = repository(ALPHA);
            CommandResult firstStepResult = alpha.addContentInfo(LOREM_IPSUM.getInfo());
            CommandResult secondStepResult = alpha.addContent(firstStepResult.getTransactionId(),
                                                              LOREM_IPSUM.getInfo().getContent(),
                                                              inputStream);

            assertThat(firstStepResult.getOperation()).isEqualTo(Operation.CREATE);
            assertThat(secondStepResult).isEqualTo(firstStepResult);
            assertHas(alpha, LOREM_IPSUM);
        }
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT, expectedExceptions = ConflictException.class)
    public void putAlreadyStoredTest() throws IOException {
        repository(ALPHA).addContentInfo(LOREM_IPSUM.getInfo());
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT)
    public void getContentTest() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                InputStream inputStream = repository(ALPHA).getContent(LOREM_IPSUM.getHash())) {
            copy(inputStream, outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(LOREM_IPSUM.getBytes());
        }
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT)
    public void getInfoHeadTest() {
        List<ContentInfo> head = repository(ALPHA).getContentInfoHead(LOREM_IPSUM.getHash());
        assertThat(head).containsExactly(LOREM_IPSUM.getInfo());
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT, expectedExceptions = UnknownContentException.class)
    public void getContentWithUnknownHashTest() {
        repository(ALPHA).getContent(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT, expectedExceptions = UnknownContentException.class)
    public void getInfoTreeWithUnknownHashTest() {
        repository(ALPHA).getContentInfoTree(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT)
    public void createReplicationTest() {
        repositoriesService.createReplication(ALPHA, BETA);

        ReplicationInfo info = getOnlyElement(repositoriesService.listReplicationInfos());
        assertThat(info.getSourceDef()).isEqualTo(defs.get(ALPHA));
        assertThat(info.getDestinationdef()).isEqualTo(defs.get(BETA));
        assertThat(info.isStarted()).isTrue();

        async(new Runnable() {
            @Override
            public void run() {
                assertHas(repository(BETA), LOREM_IPSUM);
                assertDone(getOnlyElement(repositoriesService.listReplicationInfos()).getAgentInfo());
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT)
    public void findTest() {
        async(new Runnable() {
            @Override
            public void run() {
                IndexEntry expected = new IndexEntry(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
                assertThat(repository(ALPHA).find("Lorem ipsum", 0, 10)).containsExactly(expected);
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT)
    public void getInfoTest() {
        async(new Runnable() {
            @Override
            public void run() {
                assertUpToDate(repository(ALPHA).getInfo(),
                               new RepositoryStats(1, 0, 0, ImmutableMap.of(FILE_NAME.key(), 1L)));
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = DELETE, dependsOnGroups = OPERATIONS)
    public void deleteTest() {
        Repository alpha = repository(ALPHA);
        CommandResult result = alpha.deleteContent(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
        assertThat(result.getOperation()).isEqualTo(Operation.DELETE);

        assertDeleted(alpha, LOREM_IPSUM);

        ContentInfoTree tree = alpha.getContentInfoTree(LOREM_IPSUM.getHash());
        assertThat(tree.list()).hasSize(2);
        assertThat(tree.isDeleted()).isTrue();

        async(new Runnable() {
            @Override
            public void run() {
                assertDeleted(repository(BETA), LOREM_IPSUM);

                assertThat(repository(BETA).find("Lorem ipsum", 0, 10)).isEmpty();

                assertUpToDate(repository(ALPHA).getInfo(),
                               new RepositoryStats(1, 0, 1, Collections.<String, Long>emptyMap()));
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = DELETE, dependsOnGroups = OPERATIONS, dependsOnMethods = "deleteTest")
    public void deleteAlreadyDeletedTest() {
        ContentInfo rev0 = LOREM_IPSUM.getInfo();
        ContentInfo rev1 = new ContentInfoBuilder()
                .withContent(rev0.getContent())
                .withLength(rev0.getLength())
                .withParent(rev0.getRevision())
                .withDeleted(true)
                .computeRevisionAndBuild();

        ContentInfoTree tree = new ContentInfoTreeBuilder()
                .add(rev0)
                .add(rev1)
                .build();

        CommandResult result = repository(ALPHA).deleteContent(tree.getContent(), tree.getHead());
        assertThat(result.isNoOp()).isTrue();
    }

    /**
     * Test.
     */
    @Test(groups = DELETE, dependsOnGroups = OPERATIONS, expectedExceptions = UnknownContentException.class)
    public void deleteUnknownHashTest() {
        repository(ALPHA).deleteContent(UNKNOWN_HASH, new TreeSet<Hash>());
    }

    /**
     * Test.
     */
    @Test(dependsOnGroups = DELETE)
    public void closeTest() {
        final Repository alpha = repository(ALPHA);
        repositoriesService.closeRepository(ALPHA);
        assertOpen(alpha, false);
        assertReplicationStarted(false);

        assertThrows(RepositoryClosedException.class, new Runnable() {
            @Override
            public void run() {
                alpha.getContentInfoTree(UNKNOWN_HASH);
            }
        });
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "closeTest")
    public void reopenTest() {
        repositoriesService.openRepository(ALPHA);
        Repository alpha = repository(ALPHA);
        assertOpen(alpha, true);
        assertReplicationStarted(true);

        async(new Runnable() {
            @Override
            public void run() {
                assertUpToDate(repository(ALPHA).getInfo(),
                               new RepositoryStats(1, 0, 1, Collections.<String, Long>emptyMap()));
            }
        });
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "reopenTest")
    public void closeDestinationRepositoryTest() {
        repositoriesService.closeRepository(BETA);
        assertReplicationStarted(false);
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private void assertHas(Repository repository, Content Content) {
        List<Event> events = repository.history(true, 0, 10);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getOperation()).isEqualTo(Operation.CREATE);
        assertThat(events.get(0).getContent()).isEqualTo(Content.getHash());
    }

    private void assertOpen(Repository repository, boolean expected) {
        String key = repository.getDef().getGuid().asHexadecimalString();
        assertThat(repositoriesService.getRepositoryInfo(key).isOpen()).isEqualTo(expected);
        assertThat(repository.getInfo().isOpen()).isEqualTo(expected);
    }

    private void assertReplicationStarted(boolean expected) {
        boolean actual = getOnlyElement(repositoriesService.listReplicationInfos()).isStarted();
        assertThat(actual).isEqualTo(expected);
    }

    private void assertDeleted(Repository repository, Content Content) {
        List<Event> events = repository.history(true, 0, 10);

        assertThat(events).hasSize(2);
        assertThat(events.get(1).getOperation()).isEqualTo(Operation.DELETE);
        assertThat(events.get(1).getContent()).isEqualTo(Content.getHash());
    }

    private static void assertDone(AgentInfo info) {
        assertThat(info.getState()).isEqualTo(AgentState.WAITING);
        assertThat(info.getCurSeq()).isEqualTo(info.getMaxSeq());
    }

    private static void assertUpToDate(RepositoryInfo info, RepositoryStats stats) {
        assertDone(info.getIndexingInfo());
        assertDone(info.getStatsInfo());
        assertThat(info.getStats()).isEqualTo(stats);
    }

    private static void assertThrows(Class<? extends Throwable> clazz, Runnable runnable) {
        try {
            runnable.run();

        } catch (Throwable e) {
            if (clazz.isAssignableFrom(e.getClass())) {
                // Expected case.
                return;
            }
            throw e;
        }
        throw new AssertionError("Expected an instance of " + clazz.getSimpleName() + " to be thrown");
    }

    private static void async(Runnable runnable) {
        int timeout = 60;
        int delay = 1;
        int time = 0;
        while (true) {
            try {
                wait(delay);
                time += delay;
                delay *= 2;
                if (delay > 5) {
                    delay = 5;
                }
                runnable.run();
                return;

            } catch (Exception e) {
                if (time >= timeout) {
                    throw new AssertionError(e);
                }
            } catch (AssertionError e) {
                if (time >= timeout) {
                    throw e;
                }
            } catch (Error e) {
                throw e;
            }
        }
    }

    private static void wait(int seconds) {
        try {
            Thread.sleep(1000 * seconds);

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
