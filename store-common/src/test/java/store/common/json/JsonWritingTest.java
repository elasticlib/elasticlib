package store.common.json;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
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
import static store.common.json.JsonWriting.write;
import static store.common.json.JsonWriting.writeAll;

/**
 * Unit tests.
 */
public class JsonWritingTest {

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTest() {
        for (int i = 0; i < CONTENT_INFOS.size(); i++) {
            assertThat(write(CONTENT_INFOS.get(i))).isEqualTo(CONTENT_INFOS_JSON.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTreeTest() {
        assertThat(write(CONTENT_INFO_TREE)).isEqualTo(CONTENT_INFO_TREE_JSON);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllEventsTest() {
        assertThat(writeAll(EVENTS)).isEqualTo(EVENTS_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void writeCommandResultTest() {
        for (int i = 0; i < COMMAND_RESULTS.size(); i++) {
            assertThat(write(COMMAND_RESULTS.get(i))).isEqualTo(COMMAND_RESULTS_JSON.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeAllIndexEntriesTest() {
        assertThat(writeAll(INDEX_ENTRIES)).isEqualTo(INDEX_ENTRIES_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllRepositoryDefsTest() {
        assertThat(writeAll(REPOSITORY_DEFS)).isEqualTo(REPOSITORY_DEFS_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllReplicationDefsTest() {
        assertThat(writeAll(REPLICATION_DEFS)).isEqualTo(REPLICATION_DEFS_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void writeRepositoryInfoTest() {
        for (int i = 0; i < REPOSITORY_INFOS.size(); i++) {
            assertThat(write(REPOSITORY_INFOS.get(i))).isEqualTo(REPOSITORY_INFOS_JSON.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeReplicationInfoTest() {
        for (int i = 0; i < REPLICATION_INFOS.size(); i++) {
            assertThat(write(REPLICATION_INFOS.get(i))).isEqualTo(REPLICATION_INFOS_JSON.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeAllNodeDefsTest() {
        assertThat(writeAll(NODE_DEFS)).isEqualTo(NODE_DEFS_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllNodeInfosTest() {
        assertThat(writeAll(NODE_INFOS)).isEqualTo(NODE_INFOS_ARRAY);
    }
}
