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
import static org.elasticlib.common.json.JsonTestData.COMMAND_RESULTS_JSON;
import static org.elasticlib.common.json.JsonTestData.CONTENT_INFO_JSON;
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
import static org.elasticlib.common.json.JsonWriting.write;
import static org.elasticlib.common.json.JsonWriting.writeAll;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class JsonWritingTest {

    /**
     * Test.
     */
    @Test
    public void writeStagingInfoTest() {
        assertThat(write(STAGING_INFO)).isEqualTo(STAGING_INFO_JSON);
    }

    /**
     * Test.
     */
    @Test
    public void writeRevisionTest() {
        for (int i = 0; i < REVISIONS.size(); i++) {
            assertThat(write(REVISIONS.get(i))).isEqualTo(REVISIONS_JSON.get(i));
        }
    }

    /**
     * Test.
     */
    @Test
    public void writeRevisionTreeTest() {
        assertThat(write(REVISION_TREE)).isEqualTo(REVISION_TREE_JSON);
    }

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTest() {
        assertThat(write(CONTENT_INFO)).isEqualTo(CONTENT_INFO_JSON);
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

    /**
     * Test.
     */
    @Test
    public void writeAllRemoteInfosTest() {
        assertThat(writeAll(REMOTE_INFOS)).isEqualTo(REMOTE_INFOS_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void writeAllNodeExceptionsTest() {
        assertThat(writeAll(NODE_EXCEPTIONS)).isEqualTo(NODE_EXCEPTIONS_ARRAY);
    }
}
