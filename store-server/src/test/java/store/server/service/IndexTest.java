package store.server.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import static store.server.TestUtil.LOREM_IPSUM;
import static store.server.TestUtil.UNKNOWN_HASH;
import static store.server.TestUtil.recursiveDelete;

/**
 * Unit tests.
 */
@Test(singleThreaded = true)
public class IndexTest {

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
        index = Index.create("test-index", path.resolve("index"));
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
     *
     * @throws IOException If an IO error occurs.
     */
    @Test(dependsOnGroups = "emptyRead")
    public void put() throws IOException {
        ContentInfoTree tree = new ContentInfoTreeBuilder().add(LOREM_IPSUM.getInfo()).build();
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            index.put(tree, inputStream);
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
    @Test(dependsOnGroups = "read")
    public void delete() {
        index.delete(LOREM_IPSUM.getHash());
        assertThat(index.find("lorem", 0, 20)).isEmpty();
    }
}
