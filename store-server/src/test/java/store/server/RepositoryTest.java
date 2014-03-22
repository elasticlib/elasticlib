package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.Hash;
import static store.server.TestUtil.recursiveDelete;
import store.server.exception.UnknownContentException;
import store.server.volume.RevSpec;

/**
 * Unit tests.
 */
public class RepositoryTest {

    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
    private static final Content LOREM_IPSUM = new Content("loremIpsum.txt");
    private RepositoriesService repositoriesService;
    private Path path;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(this.getClass().getName());
        Files.createDirectory(path.resolve("home"));
        repositoriesService = new RepositoriesService(path.resolve("home"), new ReplicationService());
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
    @Test
    public void createRepositoryTest() {
        repositoriesService.createRepository(path.resolve(SOURCE));
        repositoriesService.createRepository(path.resolve(DESTINATION));

        assertThat(repository(SOURCE)).isNotNull();
        assertThat(repository(DESTINATION)).isNotNull();
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnMethods = "createRepositoryTest")
    public void findTest() throws IOException {
        Repository source = repository(SOURCE);
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            source.put(LOREM_IPSUM.getInfo(), inputStream, RevSpec.none());
        }
        wait(1);
        assertThat(source.find("Lorem ipsum", 0, 10)).containsExactly(LOREM_IPSUM.getHash());
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnMethods = "findTest")
    public void createReplicationTest() throws IOException {
        repositoriesService.createReplication(SOURCE, DESTINATION);
        wait(1);
        assertThat(hasContent(repository(DESTINATION), LOREM_IPSUM.getHash()));
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private void wait(int seconds) {
        try {
            Thread.sleep(seconds * 1000);

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean hasContent(Repository repository, Hash hash) {
        try {
            InputStream inputStream = repository.getContent(hash);
            try {
                inputStream.close();
            } catch (IOException e) {
                // Impossible
                throw new AssertionError(e);
            }
        } catch (UnknownContentException e) {
            return false;
        }
        return true;
    }
}
