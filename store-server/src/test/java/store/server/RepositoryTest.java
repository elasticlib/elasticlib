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
        final Repository source = repository(SOURCE);
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            source.put(LOREM_IPSUM.getInfo(), inputStream, RevSpec.none());
        }
        async(new Assertion() {
            @Override
            public void apply() throws AssertionError {
                assertThat(source.find("Lorem ipsum", 0, 10)).containsExactly(LOREM_IPSUM.getHash());
            }
        });
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnMethods = "findTest")
    public void createReplicationTest() throws IOException {
        repositoriesService.createReplication(SOURCE, DESTINATION);
        async(new Assertion() {
            @Override
            public void apply() throws AssertionError {
                List<Event> events = repository(DESTINATION).history(true, 0, 10);

                assertThat(events).hasSize(1);
                assertThat(events.get(0).getOperation()).isEqualTo(Operation.CREATE);
                assertThat(events.get(0).getHash()).isEqualTo(LOREM_IPSUM.getHash());
            }
        });
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private void async(Assertion assertion) {
        int timeout = 60;
        int delay = 1;
        int time = 0;
        while (true) {
            try {
                wait(delay);
                assertion.apply();
                return;

            } catch (AssertionError e) {
                time += delay;
                delay *= 2;
                if (delay > 5) {
                    delay = 5;
                }
                if (time >= timeout) {
                    throw e;
                }
            }
        }
    }

    private interface Assertion {

        void apply() throws AssertionError;
    }

    private void wait(int seconds) {
        try {
            Thread.sleep(1000 * seconds);

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
