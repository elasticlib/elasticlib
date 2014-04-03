package store.common.yaml;

import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestData.*;
import static store.common.yaml.YamlTestData.*;
import static store.common.yaml.YamlWriting.write;

/**
 * Unit tests.
 */
public class YamlWritingTest {

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTest() {
        for (int i = 0; i < CONTENT_INFOS.size(); i++) {
            assertThat(write(CONTENT_INFOS.get(i))).isEqualTo(CONTENT_INFOS_YAML.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTreeTest() {
        assertThat(write(CONTENT_INFO_TREE)).isEqualTo(CONTENT_INFO_TREE_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeEventTest() {
        for (int i = 0; i < EVENTS.size(); i++) {
            assertThat(write(EVENTS.get(i))).isEqualTo(EVENTS_YAML.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeCommandResultTest() {
        for (int i = 0; i < COMMAND_RESULTS.size(); i++) {
            assertThat(write(COMMAND_RESULTS.get(i))).isEqualTo(COMMAND_RESULTS_YAML.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeIndexEntryTest() {
        for (int i = 0; i < INDEX_ENTRIES.size(); i++) {
            assertThat(write(INDEX_ENTRIES.get(i))).isEqualTo(INDEX_ENTRIES_YAML.get(i));
        }
    }
}
