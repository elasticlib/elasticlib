package store.common.yaml;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestData.COMMAND_RESULTS;
import static store.common.TestData.CONTENT_INFOS;
import static store.common.TestData.CONTENT_INFO_TREE;
import static store.common.TestData.EVENTS;
import static store.common.TestData.INDEX_ENTRIES;
import static store.common.TestData.NODE_DEFS;
import static store.common.TestData.NODE_EXCEPTIONS;
import static store.common.TestData.NODE_INFOS;
import static store.common.TestData.REPLICATION_DEFS;
import static store.common.TestData.REPLICATION_INFOS;
import static store.common.TestData.REPOSITORY_DEFS;
import static store.common.TestData.REPOSITORY_INFOS;
import static store.common.TestUtil.assertMatches;
import store.common.exception.NodeException;
import store.common.mappable.Mappable;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentInfoTree;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.common.model.ReplicationDef;
import store.common.model.ReplicationInfo;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import static store.common.yaml.YamlTestData.COMMAND_RESULTS_YAML;
import static store.common.yaml.YamlTestData.CONTENT_INFOS_YAML;
import static store.common.yaml.YamlTestData.CONTENT_INFO_TREE_YAML;
import static store.common.yaml.YamlTestData.EVENTS_YAML;
import static store.common.yaml.YamlTestData.INDEX_ENTRIES_YAML;
import static store.common.yaml.YamlTestData.NODE_DEFS_YAML;
import static store.common.yaml.YamlTestData.NODE_EXCEPTIONS_YAML;
import static store.common.yaml.YamlTestData.NODE_INFOS_YAML;
import static store.common.yaml.YamlTestData.REPLICATION_DEFS_YAML;
import static store.common.yaml.YamlTestData.REPLICATION_INFOS_YAML;
import static store.common.yaml.YamlTestData.REPOSITORY_DEFS_YAML;
import static store.common.yaml.YamlTestData.REPOSITORY_INFOS_YAML;

/**
 * Unit tests.
 */
public class YamlReadingTest {

    /**
     * Test.
     */
    @Test
    public void readContentInfoTest() {
        for (int i = 0; i < CONTENT_INFOS_YAML.size(); i++) {
            assertThat(read(CONTENT_INFOS_YAML.get(i), ContentInfo.class)).isEqualTo(CONTENT_INFOS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfoTreeTest() {
        assertThat(read(CONTENT_INFO_TREE_YAML, ContentInfoTree.class)).isEqualTo(CONTENT_INFO_TREE);
    }

    /**
     * Test.
     */
    @Test
    public void readEventTest() {
        assertThat(readAll(EVENTS_YAML, Event.class)).isEqualTo(EVENTS);
    }

    /**
     * Test.
     */
    @Test
    public void readCommandResultTest() {
        for (int i = 0; i < COMMAND_RESULTS_YAML.size(); i++) {
            assertThat(read(COMMAND_RESULTS_YAML.get(i), CommandResult.class)).isEqualTo(COMMAND_RESULTS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readIndexEntryTest() {
        assertThat(readAll(INDEX_ENTRIES_YAML, IndexEntry.class)).isEqualTo(INDEX_ENTRIES);
    }

    /**
     * Test.
     */
    @Test
    public void readAllRepositoryDefsTest() {
        assertThat(readAll(REPOSITORY_DEFS_YAML, RepositoryDef.class)).isEqualTo(REPOSITORY_DEFS);
    }

    /**
     * Test.
     */
    @Test
    public void readAllReplicationDefsTest() {
        assertThat(readAll(REPLICATION_DEFS_YAML, ReplicationDef.class)).isEqualTo(REPLICATION_DEFS);
    }

    /**
     * Test.
     */
    @Test
    public void readRepositoryInfoTest() {
        for (int i = 0; i < REPOSITORY_INFOS_YAML.size(); i++) {
            assertThat(read(REPOSITORY_INFOS_YAML.get(i), RepositoryInfo.class)).isEqualTo(REPOSITORY_INFOS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readReplicationInfoTest() {
        for (int i = 0; i < REPLICATION_INFOS_YAML.size(); i++) {
            assertThat(read(REPLICATION_INFOS_YAML.get(i), ReplicationInfo.class)).isEqualTo(REPLICATION_INFOS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readAllNodeDefsTest() {
        assertThat(readAll(NODE_DEFS_YAML, NodeDef.class)).isEqualTo(NODE_DEFS);
    }

    /**
     * Test.
     */
    @Test
    public void readAllNodeInfosTest() {
        assertThat(readAll(NODE_INFOS_YAML, NodeInfo.class)).isEqualTo(NODE_INFOS);
    }

    /**
     * Test.
     */
    @Test
    public void readAllNodeExceptionsTest() {
        assertMatches(readAll(NODE_EXCEPTIONS_YAML, NodeException.class), NODE_EXCEPTIONS);
    }

    private static <T extends Mappable> T read(String yaml, Class<T> clazz) {
        try (YamlReader yamlReader = new YamlReader(new StringReader(yaml))) {

            return yamlReader.read(clazz).get();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static <T extends Mappable> List<T> readAll(String yaml, Class<T> clazz) {
        try (YamlReader yamlReader = new YamlReader(new StringReader(yaml))) {

            return yamlReader.readAll(clazz);

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
