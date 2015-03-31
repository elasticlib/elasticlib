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
package org.elasticlib.common.json;

import static org.elasticlib.common.TestData.COMMAND_RESULTS;
import static org.elasticlib.common.TestData.CONTENT_INFO;
import static org.elasticlib.common.TestData.DIGEST;
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
import static org.elasticlib.common.json.JsonReading.read;
import static org.elasticlib.common.json.JsonReading.readAll;
import static org.elasticlib.common.json.JsonTestData.COMMAND_RESULTS_JSON;
import static org.elasticlib.common.json.JsonTestData.CONTENT_INFO_JSON;
import static org.elasticlib.common.json.JsonTestData.DIGEST_JSON;
import static org.elasticlib.common.json.JsonTestData.EVENTS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.INDEX_ENTRIES_ARRAY;
import static org.elasticlib.common.json.JsonTestData.NODE_DEFS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.NODE_EXCEPTIONS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.NODE_INFOS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.REMOTE_INFOS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.REPLICATION_DEFS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.REPLICATION_INFOS_JSON;
import static org.elasticlib.common.json.JsonTestData.REPOSITORY_DEFS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.REPOSITORY_INFOS_JSON;
import static org.elasticlib.common.json.JsonTestData.REVISIONS_JSON;
import static org.elasticlib.common.json.JsonTestData.REVISION_TREE_JSON;
import static org.elasticlib.common.json.JsonTestData.STAGING_INFO_JSON;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Digest;
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
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class JsonReadingTest {

    /**
     * Test.
     */
    @Test
    public void readDigestTest() {
        assertThat(read(DIGEST_JSON, Digest.class)).isEqualTo(DIGEST);
    }

    /**
     * Test.
     */
    @Test
    public void readStagingInfoTest() {
        assertThat(read(STAGING_INFO_JSON, StagingInfo.class)).isEqualTo(STAGING_INFO);
    }

    /**
     * Test.
     */
    @Test
    public void readRevisionTest() {
        for (int i = 0; i < REVISIONS_JSON.size(); i++) {
            assertThat(read(REVISIONS_JSON.get(i), Revision.class)).isEqualTo(REVISIONS.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void readRevisionTreeTest() {
        assertThat(read(REVISION_TREE_JSON, RevisionTree.class)).isEqualTo(REVISION_TREE);
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfoTest() {
        assertThat(read(CONTENT_INFO_JSON, ContentInfo.class)).isEqualTo(CONTENT_INFO);
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

    /**
     * Test.
     */
    @Test
    public void readAllRemoteInfosTest() {
        assertThat(readAll(REMOTE_INFOS_ARRAY, RemoteInfo.class)).isEqualTo(REMOTE_INFOS);
    }

    /**
     * Test.
     */
    @Test
    public void readAllNodeExceptionsTest() {
        assertMatches(readAll(NODE_EXCEPTIONS_ARRAY, NodeException.class), NODE_EXCEPTIONS);
    }
}
