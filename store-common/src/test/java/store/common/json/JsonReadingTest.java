package store.common.json;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import store.common.NodeDef;
import store.common.NodeInfo;
import store.common.ReplicationDef;
import store.common.ReplicationInfo;
import store.common.RepositoryDef;
import store.common.RepositoryInfo;
import static store.common.TestData.COMMAND_RESULTS;
import static store.common.TestData.CONTENT_INFOS;
import static store.common.TestData.CONTENT_INFO_TREE;
import static store.common.TestData.EVENTS;
import static store.common.TestData.INDEX_ENTRIES;
import static store.common.TestData.NODE_DEFS;
import static store.common.TestData.NODE_INFOS;
import static store.common.TestData.REPLICATION_DEFS;
import static store.common.TestData.REPLICATION_INFOS;
import static store.common.TestData.REPOSITORY_DEFS;
import static store.common.TestData.REPOSITORY_INFOS;
import static store.common.json.JsonReading.read;
import static store.common.json.JsonReading.readAll;
import static store.common.json.JsonTestData.COMMAND_RESULTS_JSON;
import static store.common.json.JsonTestData.CONTENT_INFOS_JSON;
import static store.common.json.JsonTestData.CONTENT_INFO_TREE_JSON;
import static store.common.json.JsonTestData.EVENTS_ARRAY;
import static store.common.json.JsonTestData.INDEX_ENTRIES_ARRAY;
import static store.common.json.JsonTestData.NODE_DEFS_ARRAY;
import static store.common.json.JsonTestData.NODE_INFOS_ARRAY;
import static store.common.json.JsonTestData.REPLICATION_DEFS_ARRAY;
import static store.common.json.JsonTestData.REPLICATION_INFOS_JSON;
import static store.common.json.JsonTestData.REPOSITORY_DEFS_ARRAY;
import static store.common.json.JsonTestData.REPOSITORY_INFOS_JSON;

/**
 * Unit tests.
 */
public class JsonReadingTest {

    /**
     * Test.
     */
    @Test
    public void readContentInfoTest() {
        for (int i = 0; i < CONTENT_INFOS_JSON.size(); i++) {
            assertThat(read(CONTENT_INFOS_JSON.get(i), ContentInfo.class)).isEqualTo(CONTENT_INFOS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfoTreeTest() {
        assertThat(read(CONTENT_INFO_TREE_JSON, ContentInfoTree.class)).isEqualTo(CONTENT_INFO_TREE);
    }

    /**
     * Test.
     */
    @Test
    public void readAllEventsTest() {
        assertThat(readAll(EVENTS_ARRAY, Event.class)).isEqualTo(EVENTS);
    }

    /**
     * Test.
     */
    @Test
    public void readCommandResultTest() {
        for (int i = 0; i < COMMAND_RESULTS_JSON.size(); i++) {
            assertThat(read(COMMAND_RESULTS_JSON.get(i), CommandResult.class)).isEqualTo(COMMAND_RESULTS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readAllIndexEntriesTest() {
        assertThat(readAll(INDEX_ENTRIES_ARRAY, IndexEntry.class)).isEqualTo(INDEX_ENTRIES);
    }

    /**
     * Test.
     */
    @Test
    public void readAllRepositoryDefsTest() {
        assertThat(readAll(REPOSITORY_DEFS_ARRAY, RepositoryDef.class)).isEqualTo(REPOSITORY_DEFS);
    }

    /**
     * Test.
     */
    @Test
    public void readAllReplicationDefsTest() {
        assertThat(readAll(REPLICATION_DEFS_ARRAY, ReplicationDef.class)).isEqualTo(REPLICATION_DEFS);
    }

    /**
     * Test.
     */
    @Test
    public void readRepositoryInfoTest() {
        for (int i = 0; i < REPOSITORY_INFOS_JSON.size(); i++) {
            assertThat(read(REPOSITORY_INFOS_JSON.get(i), RepositoryInfo.class)).isEqualTo(REPOSITORY_INFOS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readReplicationInfoTest() {
        for (int i = 0; i < REPLICATION_INFOS_JSON.size(); i++) {
            assertThat(read(REPLICATION_INFOS_JSON.get(i), ReplicationInfo.class)).isEqualTo(REPLICATION_INFOS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readAllNodeDefsTest() {
        assertThat(readAll(NODE_DEFS_ARRAY, NodeDef.class)).isEqualTo(NODE_DEFS);
    }

    /**
     * Test.
     */
    @Test
    public void readAllNodeInfosTest() {
        assertThat(readAll(NODE_INFOS_ARRAY, NodeInfo.class)).isEqualTo(NODE_INFOS);
    }
}
