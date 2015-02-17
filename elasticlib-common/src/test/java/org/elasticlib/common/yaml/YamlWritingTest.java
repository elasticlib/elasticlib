/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.yaml;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import static org.elasticlib.common.TestData.COMMAND_RESULTS;
import static org.elasticlib.common.TestData.CONTENT_INFO;
import static org.elasticlib.common.TestData.EVENTS;
import static org.elasticlib.common.TestData.INDEX_ENTRIES;
import static org.elasticlib.common.TestData.NODE_DEFS;
import static org.elasticlib.common.TestData.NODE_EXCEPTIONS;
import static org.elasticlib.common.TestData.NODE_INFOS;
import static org.elasticlib.common.TestData.REPLICATION_DEFS;
import static org.elasticlib.common.TestData.REPLICATION_INFOS;
import static org.elasticlib.common.TestData.REPOSITORY_DEFS;
import static org.elasticlib.common.TestData.REPOSITORY_INFOS;
import static org.elasticlib.common.TestData.REVISIONS;
import static org.elasticlib.common.TestData.REVISION_TREE;
import static org.elasticlib.common.TestData.STAGING_INFO;
import org.elasticlib.common.mappable.Mappable;
import static org.elasticlib.common.yaml.YamlTestData.COMMAND_RESULTS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.CONTENT_INFO_YAML;
import static org.elasticlib.common.yaml.YamlTestData.EVENTS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.INDEX_ENTRIES_YAML;
import static org.elasticlib.common.yaml.YamlTestData.NODE_DEFS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.NODE_EXCEPTIONS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.NODE_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPLICATION_DEFS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPLICATION_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPOSITORY_DEFS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPOSITORY_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REVISIONS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REVISION_TREE_YAML;
import static org.elasticlib.common.yaml.YamlTestData.STAGING_INFO_YAML;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import static org.yaml.snakeyaml.DumperOptions.LineBreak.UNIX;

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
    public void writeContentInfoTest() {
        assertThat(write(CONTENT_INFO)).isEqualTo(CONTENT_INFO_YAML);
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
                YamlWriter yamlWriter = new YamlWriter(stringWriter, UNIX)) {

            yamlWriter.write(mappable);
            return stringWriter.toString().trim();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static String writeAll(List<? extends Mappable> mappables) {
        try (Writer stringWriter = new StringWriter();
                YamlWriter yamlWriter = new YamlWriter(stringWriter, UNIX)) {

            yamlWriter.writeAll(mappables);
            return stringWriter.toString().trim();

        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
