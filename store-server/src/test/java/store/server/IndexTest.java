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

/**
 * Unit tests.
 */
@Test(singleThreaded = true)
public class IndexTest {

    private static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    private static final Content LOREM_IPSUM = new Content("loremIpsum.txt");
    private Path path;
    private Index index;

    /**
     * Initialization.
     *
     * @throws IOException If an IO error occurs.
     */
    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(getClass().getSimpleName() + "-");
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
    public void create() {
        index = Index.create(path.resolve("index"));
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "create")
    public void findUnknown() {
        assertThat(index.find("UNKNOWN", 0, 20)).isEmpty();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "create")
    public void containsUnknown() {
        assertThat(index.contains(UNKNOWN_HASH)).isFalse();
    }

    /**
     * Test.
     */
    @Test(dependsOnMethods = "create")
    public void deleteUnknown() {
        index.delete(UNKNOWN_HASH);
    }

    /**
     * Test.
     */
    @Test(groups = "emptyRead", dependsOnMethods = "create")
    public void findOnEmptyIndex() {
        assertThat(index.find("lorem", 0, 20)).isEmpty();
    }

    /**
     * Test.
     */
    @Test(groups = "emptyRead", dependsOnMethods = "create")
    public void containsOnEmptyIndex() {
        assertThat(index.contains(LOREM_IPSUM.getHash())).isFalse();
    }

    /**
     * Test.
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnGroups = "emptyRead")
    public void put() throws IOException {
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            index.put(LOREM_IPSUM.getInfo(), inputStream);
        }
    }

    /**
     * Test.
     */
    @Test(groups = "read", dependsOnMethods = "put")
    public void find() {
        assertThat(index.find("lorem", 0, 20)).hasSize(1);
    }

    /**
     * Test.
     */
    @Test(groups = "read", dependsOnMethods = "put")
    public void contains() {
        assertThat(index.contains(LOREM_IPSUM.getHash())).isTrue();
    }

    /**
     * Test.
     */
    @Test(dependsOnGroups = "read")
    public void delete() {
        index.delete(LOREM_IPSUM.getHash());
        assertThat(index.contains(LOREM_IPSUM.getHash())).isFalse();
    }
}
