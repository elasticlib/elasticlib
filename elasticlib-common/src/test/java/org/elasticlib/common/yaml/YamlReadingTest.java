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
import java.io.StringReader;
import java.util.List;
import static org.elasticlib.common.TestData.COMMAND_RESULTS;
import static org.elasticlib.common.TestData.CONTENT_INFO;
import static org.elasticlib.common.TestData.EVENTS;
import static org.elasticlib.common.TestData.INDEX_ENTRIES;
import static org.elasticlib.common.TestData.NODE_DEFS;
import static org.elasticlib.common.TestData.NODE_EXCEPTIONS;
import static org.elasticlib.common.TestData.NODE_INFOS;
import static org.elasticlib.common.TestData.REMOTE_INFOS;
import static org.elasticlib.common.TestData.REPLICATION_DEFS;
import static org.elasticlib.common.TestData.REPLICATION_INFOS;
import static org.elasticlib.common.TestData.REPOSITORY_DEFS;
import static org.elasticlib.common.TestData.REPOSITORY_INFOS;
import static org.elasticlib.common.TestData.REVISIONS;
import static org.elasticlib.common.TestData.REVISION_TREE;
import static org.elasticlib.common.TestData.STAGING_INFO;
import static org.elasticlib.common.TestUtil.assertMatches;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.ReplicationDef;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import static org.elasticlib.common.yaml.YamlTestData.COMMAND_RESULTS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.CONTENT_INFO_YAML;
import static org.elasticlib.common.yaml.YamlTestData.EVENTS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.INDEX_ENTRIES_YAML;
import static org.elasticlib.common.yaml.YamlTestData.NODE_DEFS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.NODE_EXCEPTIONS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.NODE_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REMOTE_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPLICATION_DEFS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPLICATION_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPOSITORY_DEFS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REPOSITORY_INFOS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REVISIONS_YAML;
import static org.elasticlib.common.yaml.YamlTestData.REVISION_TREE_YAML;
import static org.elasticlib.common.yaml.YamlTestData.STAGING_INFO_YAML;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class YamlReadingTest {

    /**
     * Test.
     */
    @Test
    public void readStagingInfoTest() {
        assertThat(read(STAGING_INFO_YAML, StagingInfo.class)).isEqualTo(STAGING_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void readRevisionTest() {
        for (int i = 0; i < REVISIONS_YAML.size(); i++) {
            assertThat(read(REVISIONS_YAML.get(i), Revision.class)).isEqualTo(REVISIONS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readRevisionTreeTest() {
        assertThat(read(REVISION_TREE_YAML, RevisionTree.class)).isEqualTo(REVISION_TREE);
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfoTest() {
        assertThat(read(CONTENT_INFO_YAML, ContentInfo.class)).isEqualTo(CONTENT_INFO);
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
    public void readAllRemoteInfosTest() {
        assertThat(readAll(REMOTE_INFOS_YAML, RemoteInfo.class)).isEqualTo(REMOTE_INFOS);
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
