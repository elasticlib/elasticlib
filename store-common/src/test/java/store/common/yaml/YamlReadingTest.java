package store.common.yaml;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import store.common.ReplicationDef;
import store.common.RepositoryDef;
import static store.common.TestData.*;
import static store.common.yaml.YamlReading.read;
import static store.common.yaml.YamlReading.readAll;
import static store.common.yaml.YamlTestData.*;

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
}