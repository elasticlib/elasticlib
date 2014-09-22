package store.server.repository;

import com.google.common.collect.ImmutableMap;
import static com.google.common.collect.Iterables.getLast;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.AgentInfo;
import store.common.AgentState;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.Event;
import store.common.IndexEntry;
import static store.common.IoUtil.copy;
import store.common.Operation;
import store.common.RepositoryDef;
import store.common.RepositoryInfo;
import store.common.RepositoryStats;
import store.common.config.Config;
import store.common.hash.Hash;
import static store.common.metadata.Properties.Common.FILE_NAME;
import store.common.value.Value;
import store.server.Content;
import static store.server.TestUtil.LOREM_IPSUM;
import static store.server.TestUtil.UNKNOWN_HASH;
import static store.server.TestUtil.async;
import static store.server.TestUtil.config;
import static store.server.TestUtil.recursiveDelete;
import store.server.exception.ConflictException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.RepositoryClosedException;
import store.server.exception.UnknownContentException;
import store.server.manager.ManagerModule;

/**
 * Unit tests.
 */
public class RepositoryTest {

    private static final String CREATE_REPOSITORY = "createRepository";
    private static final String CREATE_REPOSITORY_CHECKS = "createRepositoryChecks";
    private static final String ADD_CONTENT = "addContent";
    private static final String ADD_CONTENT_CHECKS = "addContentChecks";
    private static final String UPDATE_CONTENT = "updateContent";
    private static final String UPDATE_CONTENT_CHECKS = "updateContentChecks";
    private static final String DELETE_CONTENT = "deleteContent";
    private static final String DELETE_CONTENT_CHECKS = "deleteContentChecks";
    private static final String CLOSE_REPOSITORY = "closeRepository";
    private static final String CLOSE_REPOSITORY_CHECKS = "closeRepositoryChecks";
    private static final String OPEN_REPOSITORY = "openRepository";
    private static final String OPEN_REPOSITORY_CHECKS = "openRepositoryChecks";
    private static final String REPOSITORY = "repository";
    private static final String DESCRIPTION = "description";
    private static final String TEST = "test";
    private static final String LOREM_IPSUM_QUERY = "Lorem ipsum";
    private static final Content UPDATED_LOREM_IPSUM = LOREM_IPSUM.add(DESCRIPTION, Value.of(TEST));
    private static final Content DELETED_LOREM_IPSUM = UPDATED_LOREM_IPSUM.delete();
    private Config config;
    private Path path;
    private Path repositoryPath;
    private ManagerModule managerModule;
    private Repository repository;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        config = config();
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        repositoryPath = path.resolve(REPOSITORY);
        managerModule = new ManagerModule(path.resolve("home"), config);

        managerModule.start();
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
        if (repository != null) {
            repository.close();
        }
        managerModule.stop();
        recursiveDelete(path);
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY)
    public void createRepositoryTest() {
        repository = Repository.create(repositoryPath,
                                       config,
                                       managerModule.getTaskManager(),
                                       managerModule.getMessageManager());

        RepositoryDef def = repository.getDef();
        assertThat(def.getName()).isEqualTo(REPOSITORY);
        assertThat(def.getPath()).isEqualTo(repositoryPath);

        RepositoryInfo info = repository.getInfo();
        assertThat(info.getDef()).isEqualTo(def);
        assertThat(info.isOpen()).isTrue();

        assertThat(repository.history(true, 0, 10)).isEmpty();

        async(new Runnable() {
            @Override
            public void run() {
                assertUpToDate(repository.getInfo(),
                               new RepositoryStats(0, 0, 0, Collections.<String, Long>emptyMap()));
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS,
          dependsOnGroups = CREATE_REPOSITORY,
          expectedExceptions = InvalidRepositoryPathException.class)
    public void createRepositoryAlreadyExistsTest() {
        Repository.create(repositoryPath,
                          config,
                          managerModule.getTaskManager(),
                          managerModule.getMessageManager());
    }

    /**
     * Test.
     */
    @Test(groups = CREATE_REPOSITORY_CHECKS,
          dependsOnGroups = CREATE_REPOSITORY,
          expectedExceptions = ConflictException.class)
    public void addContentWithUnknownParentTest() {
        repository.addContentInfo(UPDATED_LOREM_IPSUM.getInfo());
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT, dependsOnGroups = CREATE_REPOSITORY_CHECKS)
    public void addContentTest() throws IOException {
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            CommandResult firstStepResult = repository.addContentInfo(LOREM_IPSUM.getInfo());
            CommandResult secondStepResult = repository.addContent(firstStepResult.getTransactionId(),
                                                                   LOREM_IPSUM.getInfo().getContent(),
                                                                   inputStream);

            assertThat(firstStepResult.getOperation()).isEqualTo(Operation.CREATE);
            assertThat(secondStepResult).isEqualTo(firstStepResult);
        }
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentTest() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                InputStream inputStream = repository.getContent(LOREM_IPSUM.getHash())) {
            copy(inputStream, outputStream);
            assertThat(outputStream.toByteArray()).isEqualTo(LOREM_IPSUM.getBytes());
        }
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS,
          dependsOnGroups = ADD_CONTENT,
          expectedExceptions = UnknownContentException.class)
    public void getContentWithUnknownHashTest() {
        repository.getContent(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentInfoHeadTest() {
        List<ContentInfo> head = repository.getContentInfoHead(LOREM_IPSUM.getHash());
        assertThat(head).containsExactly(LOREM_IPSUM.getInfo());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getContentInfoTreeTest() {
        assertThat(repository.getContentInfoTree(LOREM_IPSUM.getHash())).isEqualTo(LOREM_IPSUM.getTree());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS,
          dependsOnGroups = ADD_CONTENT,
          expectedExceptions = UnknownContentException.class)
    public void getContentInfoTreeWithUnknownHashTest() {
        repository.getContentInfoTree(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void historyTest() {
        List<Event> events = repository.history(true, 0, 10);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getOperation()).isEqualTo(Operation.CREATE);
        assertThat(events.get(0).getContent()).isEqualTo(LOREM_IPSUM.getHash());
        assertThat(events.get(0).getRevisions()).isEqualTo(LOREM_IPSUM.getHead());
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void findTest() {
        async(new Runnable() {
            @Override
            public void run() {
                IndexEntry expected = new IndexEntry(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
                assertThat(repository.find(LOREM_IPSUM_QUERY, 0, 10)).containsExactly(expected);
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = ADD_CONTENT_CHECKS, dependsOnGroups = ADD_CONTENT)
    public void getInfoTest() {
        async(new Runnable() {
            @Override
            public void run() {
                assertUpToDate(repository.getInfo(),
                               new RepositoryStats(1, 0, 0, ImmutableMap.of(FILE_NAME.key(), 1L)));
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT, dependsOnGroups = ADD_CONTENT_CHECKS)
    public void updateTest() {
        CommandResult result = repository.addContentInfo(UPDATED_LOREM_IPSUM.getInfo());
        assertThat(result.getOperation()).isEqualTo(Operation.UPDATE);
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS,
          dependsOnGroups = UPDATE_CONTENT,
          expectedExceptions = ConflictException.class)
    public void updateWithConflictTest() {
        repository.addContentInfo(LOREM_IPSUM.add(DESCRIPTION, Value.of("conflict")).getInfo());
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void updateAlreadyUpdatedTest() {
        CommandResult result = repository.addContentInfo(UPDATED_LOREM_IPSUM.getInfo());
        assertThat(result.isNoOp()).isTrue();
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void getContentInfoTreeAfterUpdateTest() {
        assertThat(repository.getContentInfoTree(LOREM_IPSUM.getHash())).isEqualTo(UPDATED_LOREM_IPSUM.getTree());
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void historyAfterUpdateTest() {
        List<Event> events = repository.history(true, 0, 10);
        assertThat(events).hasSize(2);

        Event latest = getLast(events);
        assertThat(latest.getOperation()).isEqualTo(Operation.UPDATE);
        assertThat(latest.getContent()).isEqualTo(UPDATED_LOREM_IPSUM.getHash());
        assertThat(latest.getRevisions()).isEqualTo(UPDATED_LOREM_IPSUM.getHead());
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void findAfterUpdateTest() {
        async(new Runnable() {
            @Override
            public void run() {
                IndexEntry expected = new IndexEntry(UPDATED_LOREM_IPSUM.getHash(), UPDATED_LOREM_IPSUM.getHead());
                assertThat(repository.find(DESCRIPTION + ":" + TEST, 0, 10)).containsExactly(expected);
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = UPDATE_CONTENT_CHECKS, dependsOnGroups = UPDATE_CONTENT)
    public void getInfoAfterUpdateTest() {
        async(new Runnable() {
            @Override
            public void run() {
                assertUpToDate(repository.getInfo(),
                               new RepositoryStats(1, 1, 0, ImmutableMap.of(FILE_NAME.key(), 1L,
                                                                            DESCRIPTION, 1L)));
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT, dependsOnGroups = UPDATE_CONTENT_CHECKS)
    public void deleteTest() {
        CommandResult result = repository.deleteContent(LOREM_IPSUM.getHash(), UPDATED_LOREM_IPSUM.getHead());
        assertThat(result.getOperation()).isEqualTo(Operation.DELETE);
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void deleteAlreadyDeletedTest() {
        CommandResult result = repository.deleteContent(LOREM_IPSUM.getHash(), DELETED_LOREM_IPSUM.getHead());
        assertThat(result.isNoOp()).isTrue();
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS,
          dependsOnGroups = DELETE_CONTENT,
          expectedExceptions = UnknownContentException.class)
    public void deleteUnknownHashTest() {
        repository.deleteContent(UNKNOWN_HASH, new TreeSet<Hash>());
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void getContentInfoTreeAfterDeleteTest() {
        assertThat(repository.getContentInfoTree(LOREM_IPSUM.getHash())).isEqualTo(DELETED_LOREM_IPSUM.getTree());
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void historyAfterDeleteTest() {
        List<Event> events = repository.history(true, 0, 10);
        assertThat(events).hasSize(3);

        Event latest = getLast(events);
        assertThat(latest.getOperation()).isEqualTo(Operation.DELETE);
        assertThat(latest.getContent()).isEqualTo(DELETED_LOREM_IPSUM.getHash());
        assertThat(latest.getRevisions()).isEqualTo(DELETED_LOREM_IPSUM.getHead());
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void findAfterDeleteTest() {
        async(new Runnable() {
            @Override
            public void run() {
                assertThat(repository.find(LOREM_IPSUM_QUERY, 0, 10)).isEmpty();
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = DELETE_CONTENT_CHECKS, dependsOnGroups = DELETE_CONTENT)
    public void getInfoAfterDeleteTest() {
        async(new Runnable() {
            @Override
            public void run() {
                assertUpToDate(repository.getInfo(),
                               new RepositoryStats(1, 1, 1, Collections.<String, Long>emptyMap()));
            }
        });
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY, dependsOnGroups = DELETE_CONTENT_CHECKS)
    public void closeRepositoryTest() {
        repository.close();
        assertThat(repository.getInfo().isOpen()).isFalse();
    }

    /**
     * Test.
     */
    @Test(groups = CLOSE_REPOSITORY_CHECKS,
          dependsOnGroups = CLOSE_REPOSITORY,
          expectedExceptions = RepositoryClosedException.class)
    public void getContentInfoTreeAfterCloseTest() {
        repository.getContentInfoTree(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY, dependsOnGroups = CLOSE_REPOSITORY_CHECKS)
    public void openRepositoryTest() {
        repository = Repository.open(repositoryPath,
                                     config,
                                     managerModule.getTaskManager(),
                                     managerModule.getMessageManager());

        assertThat(repository.getInfo().isOpen()).isTrue();
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS,
          dependsOnGroups = OPEN_REPOSITORY,
          expectedExceptions = InvalidRepositoryPathException.class)
    public void openRepositoryUnknownTest() {
        repository = Repository.open(path.resolve("unknown"),
                                     config,
                                     managerModule.getTaskManager(),
                                     managerModule.getMessageManager());
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS, dependsOnGroups = OPEN_REPOSITORY)
    public void historyAfterOpenTest() {
        historyAfterDeleteTest();
    }

    /**
     * Test.
     */
    @Test(groups = OPEN_REPOSITORY_CHECKS, dependsOnGroups = OPEN_REPOSITORY)
    public void getInfoAfterOpenTest() {
        // Actually, we want to check state has not changed.
        getInfoAfterDeleteTest();
    }

    private static void assertUpToDate(RepositoryInfo info, RepositoryStats stats) {
        assertDone(info.getIndexingInfo());
        assertDone(info.getStatsInfo());
        assertThat(info.getStats()).isEqualTo(stats);
    }

    private static void assertDone(AgentInfo info) {
        assertThat(info.getState()).isEqualTo(AgentState.WAITING);
        assertThat(info.getCurSeq()).isEqualTo(info.getMaxSeq());
    }
}
