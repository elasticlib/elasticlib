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

@Test(singleThreaded = true)
public class IndexTest {

    private static final Hash UNKNOWN_HASH = new Hash("88cd962fec779a3abafa95aad8ace74cae767427");
    private static final Content LOREM_IPSUM = new Content("loremIpsum.txt");
    private Path path;
    private Index index;

    @BeforeClass
    public void init() throws IOException {
        path = Files.createTempDirectory(this.getClass().getName());
    }

    @AfterClass
    public void cleanUp() throws IOException {
        recursiveDelete(path);
    }

    @Test
    public void create() {
        index = Index.create(path.resolve("index"));
    }

    @Test(dependsOnMethods = "create")
    public void findUnknown() {
        assertThat(index.find("UNKNOWN")).isEmpty();
    }

    @Test(dependsOnMethods = "create")
    public void containsUnknown() {
        assertThat(index.contains(UNKNOWN_HASH)).isFalse();
    }

    @Test(dependsOnMethods = "create")
    public void deleteUnknown() {
        index.delete(UNKNOWN_HASH);
    }

    @Test(groups = "emptyRead", dependsOnMethods = "create")
    public void findOnEmptyIndex() {
        assertThat(index.find("lorem")).isEmpty();
    }

    @Test(groups = "emptyRead", dependsOnMethods = "create")
    public void containsOnEmptyIndex() {
        assertThat(index.contains(LOREM_IPSUM.getHash())).isFalse();
    }

    @Test(dependsOnGroups = "emptyRead")
    public void put() throws IOException {
        try (InputStream inputStream = LOREM_IPSUM.getInputStream()) {
            index.put(LOREM_IPSUM.getInfo(), inputStream);
        }
    }

    @Test(groups = "read", dependsOnMethods = "put")
    public void find() {
        assertThat(index.find("lorem")).hasSize(1);
    }

    @Test(groups = "read", dependsOnMethods = "put")
    public void contains() {
        assertThat(index.contains(LOREM_IPSUM.getHash())).isTrue();
    }

    @Test(dependsOnGroups = "read")
    public void delete() {
        index.delete(LOREM_IPSUM.getHash());
        assertThat(index.contains(LOREM_IPSUM.getHash())).isFalse();
    }
}
