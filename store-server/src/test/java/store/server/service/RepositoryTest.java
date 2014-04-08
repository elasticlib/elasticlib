package store.server.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.Event;
import store.common.IndexEntry;
import static store.common.IoUtil.copy;
import store.common.Operation;
import store.common.ReplicationDef;
import store.common.RepositoryDef;
import store.common.hash.Hash;
import store.server.Content;
import static store.server.TestUtil.LOREM_IPSUM;
import static store.server.TestUtil.UNKNOWN_HASH;
import static store.server.TestUtil.recursiveDelete;
import store.server.exception.ConflictException;
import store.server.exception.RepositoryNotStartedException;
import store.server.exception.UnknownContentException;

/**
 * Unit tests.
 */
public class RepositoryTest {

    private static final String INIT = "init";
    private static final String OPERATIONS = "operations";
    private static final String DELETE = "delete";
    private static final String ALPHA = "alpha";
    private static final String BETA = "beta";
    private RepositoriesService repositoriesService;
    private Path path;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
        Files.createDirectory(path.resolve("home"));
        repositoriesService = new RepositoriesService(path.resolve("home"));
    }

    /**
     * Clean up.
     *
     * @throws IOException If an IO error occurs.
     */
    @AfterClass
    public void cleanUp() throws IOException {
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

        assertThat(repositoriesService.listRepositoryDefs()).containsExactly(new RepositoryDef(ALPHA, alphaPath),
                                                                            new RepositoryDef(BETA, betaPath));
        assertThat(repository(ALPHA).history(true, 0, 10)).isEmpty();
        assertThat(repository(BETA).history(true, 0, 10)).isEmpty();
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
            CommandResult firstStepResult = alpha.addInfo(LOREM_IPSUM.getInfo());
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
        repository(ALPHA).addInfo(LOREM_IPSUM.getInfo());
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
        List<ContentInfo> head = repository(ALPHA).getInfoHead(LOREM_IPSUM.getHash());
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
        repository(ALPHA).getInfoTree(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = OPERATIONS, dependsOnGroups = INIT)
    public void createReplicationTest() {
        repositoriesService.createReplication(ALPHA, BETA);
        assertThat(repositoriesService.listReplicationDefs()).containsExactly(new ReplicationDef(ALPHA, BETA));
        async(new Runnable() {
            @Override
            public void run() {
                assertHas(repository(BETA), LOREM_IPSUM);
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
    @Test(groups = DELETE, dependsOnGroups = OPERATIONS)
    public void deleteTest() {
        Repository alpha = repository(ALPHA);
        CommandResult result = alpha.deleteContent(LOREM_IPSUM.getHash(), LOREM_IPSUM.getHead());
        assertThat(result.getOperation()).isEqualTo(Operation.DELETE);

        assertDeleted(alpha, LOREM_IPSUM);

        ContentInfoTree tree = alpha.getInfoTree(LOREM_IPSUM.getHash());
        assertThat(tree.list()).hasSize(2);
        assertThat(tree.isDeleted()).isTrue();

        async(new Runnable() {
            @Override
            public void run() {
                assertDeleted(repository(BETA), LOREM_IPSUM);
            }
        });
        async(new Runnable() {
            @Override
            public void run() {
                assertThat(repository(BETA).find("Lorem ipsum", 0, 10)).isEmpty();
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
    @Test(dependsOnGroups = DELETE, expectedExceptions = RepositoryNotStartedException.class)
    public void stopTest() {
        Repository alpha = repository(ALPHA);
        alpha.stop();
        alpha.getInfoTree(UNKNOWN_HASH);
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

    private void assertDeleted(Repository repository, Content Content) {
        List<Event> events = repository.history(true, 0, 10);

        assertThat(events).hasSize(2);
        assertThat(events.get(1).getOperation()).isEqualTo(Operation.DELETE);
        assertThat(events.get(1).getContent()).isEqualTo(Content.getHash());
    }

    private void async(Runnable runnable) {
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

    private void wait(int seconds) {
        try {
            Thread.sleep(1000 * seconds);

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
