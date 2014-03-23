package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.Event;
import store.common.Operation;
import static store.server.TestUtil.recursiveDelete;
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
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
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
        Path sourcePath = path.resolve(SOURCE);
        Path destinationPath = path.resolve(DESTINATION);

        repositoriesService.createRepository(sourcePath);
        repositoriesService.createRepository(destinationPath);

        assertThat(repository(SOURCE)).isNotNull();
        assertThat(repository(DESTINATION)).isNotNull();
        assertThat(repositoriesService.getConfig().getRepositories()).containsExactly(sourcePath, destinationPath);
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnMethods = "createRepositoryTest")
    public void putTest() throws IOException {
        Repository source = repository(SOURCE);
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            source.put(LOREM_IPSUM.getInfo(), inputStream, RevSpec.none());
        }
        assertHas(source, LOREM_IPSUM);
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "putTest")
    public void createReplicationTest() {
        repositoriesService.createReplication(SOURCE, DESTINATION);

        assertThat(repositoriesService.getConfig().getSync(SOURCE)).containsExactly(DESTINATION);
        async(new Runnable() {
            @Override
            public void run() {
                assertHas(repository(DESTINATION), LOREM_IPSUM);
            }
        });
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnMethods = "createReplicationTest")
    public void findTest() throws IOException {
        async(new Runnable() {
            @Override
            public void run() {
                assertThat(repository(SOURCE).find("Lorem ipsum", 0, 10)).containsExactly(LOREM_IPSUM.getHash());
            }
        });
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private void assertHas(Repository repository, Content Content) {
        List<Event> events = repository.history(true, 0, 10);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getOperation()).isEqualTo(Operation.CREATE);
        assertThat(events.get(0).getHash()).isEqualTo(Content.getHash());
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
