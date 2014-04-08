package store.common.json;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestData.*;
import static store.common.json.JsonTestData.*;
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
}
