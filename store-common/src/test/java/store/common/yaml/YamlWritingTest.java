package store.common.yaml;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static store.common.TestData.COMMAND_RESULTS;
import static store.common.TestData.CONTENT_INFOS;
import static store.common.TestData.EVENTS;
import static store.common.TestData.INDEX_ENTRIES;
import static store.common.TestData.NODE_DEFS;
import static store.common.TestData.NODE_EXCEPTIONS;
import static store.common.TestData.NODE_INFOS;
import static store.common.TestData.REPLICATION_DEFS;
import static store.common.TestData.REPLICATION_INFOS;
import static store.common.TestData.REPOSITORY_DEFS;
import static store.common.TestData.REPOSITORY_INFOS;
import static store.common.TestData.REVISIONS;
import static store.common.TestData.REVISION_TREE;
import static store.common.TestData.STAGING_INFO;
import store.common.mappable.Mappable;
import static store.common.yaml.YamlTestData.COMMAND_RESULTS_YAML;
import static store.common.yaml.YamlTestData.CONTENT_INFOS_YAML;
import static store.common.yaml.YamlTestData.EVENTS_YAML;
import static store.common.yaml.YamlTestData.INDEX_ENTRIES_YAML;
import static store.common.yaml.YamlTestData.NODE_DEFS_YAML;
import static store.common.yaml.YamlTestData.NODE_EXCEPTIONS_YAML;
import static store.common.yaml.YamlTestData.NODE_INFOS_YAML;
import static store.common.yaml.YamlTestData.REPLICATION_DEFS_YAML;
import static store.common.yaml.YamlTestData.REPLICATION_INFOS_YAML;
import static store.common.yaml.YamlTestData.REPOSITORY_DEFS_YAML;
import static store.common.yaml.YamlTestData.REPOSITORY_INFOS_YAML;
import static store.common.yaml.YamlTestData.REVISIONS_YAML;
import static store.common.yaml.YamlTestData.REVISION_TREE_YAML;
import static store.common.yaml.YamlTestData.STAGING_INFO_YAML;

/**
 * Unit tests.
 */
public class YamlWritingTest {

    /**
     * Test.
     */
    @Test
    public void writeStagingInfoTest() {
        assertThat(write(STAGING_INFO)).isEqualTo(STAGING_INFO_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeRevisionTest() {
        for (int i = 0; i < REVISIONS.size(); i++) {
            assertThat(write(REVISIONS.get(i))).isEqualTo(REVISIONS_YAML.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeRevisionTreeTest() {
        assertThat(write(REVISION_TREE)).isEqualTo(REVISION_TREE_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllContentInfoTest() {
        assertThat(writeAll(CONTENT_INFOS)).isEqualTo(CONTENT_INFOS_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeEventTest() {
        assertThat(writeAll(EVENTS)).isEqualTo(EVENTS_YAML);
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
        assertThat(writeAll(INDEX_ENTRIES)).isEqualTo(INDEX_ENTRIES_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllRepositoryDefsTest() {
        assertThat(writeAll(REPOSITORY_DEFS)).isEqualTo(REPOSITORY_DEFS_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllReplicationDefsTest() {
        assertThat(writeAll(REPLICATION_DEFS)).isEqualTo(REPLICATION_DEFS_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeRepositoryInfoTest() {
        for (int i = 0; i < REPOSITORY_INFOS.size(); i++) {
            assertThat(write(REPOSITORY_INFOS.get(i))).isEqualTo(REPOSITORY_INFOS_YAML.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeReplicationInfoTest() {
        for (int i = 0; i < REPLICATION_INFOS.size(); i++) {
            assertThat(write(REPLICATION_INFOS.get(i))).isEqualTo(REPLICATION_INFOS_YAML.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeAllNodeDefsTest() {
        assertThat(writeAll(NODE_DEFS)).isEqualTo(NODE_DEFS_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllNodeInfosTest() {
        assertThat(writeAll(NODE_INFOS)).isEqualTo(NODE_INFOS_YAML);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllNodeExceptionsTest() {
        assertThat(writeAll(NODE_EXCEPTIONS)).isEqualTo(NODE_EXCEPTIONS_YAML);
    }

    private static String write(Mappable mappable) {
        try (Writer stringWriter = new StringWriter();
                YamlWriter yamlWriter = new YamlWriter(stringWriter)) {

            yamlWriter.write(mappable);
            return stringWriter.toString();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static String writeAll(List<? extends Mappable> mappables) {
        try (Writer stringWriter = new StringWriter();
                YamlWriter yamlWriter = new YamlWriter(stringWriter)) {

            yamlWriter.writeAll(mappables);
            return stringWriter.toString();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
