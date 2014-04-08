package store.common.json;

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
import static store.common.json.JsonReading.read;
import static store.common.json.JsonReading.readAll;
import static store.common.json.JsonTestData.*;

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
}
