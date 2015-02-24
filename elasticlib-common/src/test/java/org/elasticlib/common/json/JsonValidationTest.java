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

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.time.Instant;
import javax.json.Json;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import static org.elasticlib.common.TestUtil.array;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import static org.elasticlib.common.json.JsonTestData.COMMAND_RESULTS_JSON;
import static org.elasticlib.common.json.JsonTestData.CONTENT_INFO_JSON;
import static org.elasticlib.common.json.JsonTestData.EVENTS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.INDEX_ENTRIES_ARRAY;
import static org.elasticlib.common.json.JsonTestData.NODE_DEFS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.NODE_EXCEPTIONS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.NODE_INFOS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.REMOTE_INFOS_ARRAY;
import static org.elasticlib.common.json.JsonTestData.REPLICATION_INFOS_JSON;
import static org.elasticlib.common.json.JsonTestData.REPOSITORY_INFOS_JSON;
import static org.elasticlib.common.json.JsonTestData.REVISIONS_JSON;
import static org.elasticlib.common.json.JsonTestData.REVISION_TREE_JSON;
import static org.elasticlib.common.json.JsonTestData.STAGING_INFO_JSON;
import static org.elasticlib.common.json.JsonValidation.hasArrayValue;
import static org.elasticlib.common.json.JsonValidation.hasBooleanValue;
import static org.elasticlib.common.json.JsonValidation.hasStringValue;
import static org.elasticlib.common.json.JsonValidation.isValid;
import org.elasticlib.common.json.schema.Schema;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.Event;
import org.elasticlib.common.model.IndexEntry;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.common.model.Revision;
import org.elasticlib.common.model.RevisionTree;
import org.elasticlib.common.model.StagingInfo;
import org.elasticlib.common.value.Value;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests.
 */
public class JsonValidationTest {

    /**
     * Test.
     */
    @Test
    public void hasArrayValueTest() {
        JsonObject json = createObjectBuilder()
                .add("array", createArrayBuilder().build())
                .add("text", "value")
                .build();

        assertThat(hasArrayValue(json, "array")).isTrue();
        assertThat(hasArrayValue(json, "text")).isFalse();
        assertThat(hasArrayValue(json, "unknown")).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void hasStringValueTest() {
        JsonObject json = createObjectBuilder()
                .add("text", "value")
                .add("num", 10)
                .build();

        assertThat(hasStringValue(json, "text")).isTrue();
        assertThat(hasStringValue(json, "num")).isFalse();
        assertThat(hasStringValue(json, "unknown")).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void hasBooleanValueTest() {
        JsonObject json = createObjectBuilder()
                .add("yes", true)
                .add("no", false)
                .add("num", 10)
                .build();

        assertThat(hasBooleanValue(json, "yes")).isTrue();
        assertThat(hasBooleanValue(json, "no")).isTrue();
        assertThat(hasBooleanValue(json, "num")).isFalse();
        assertThat(hasBooleanValue(json, "unknown")).isFalse();
    }

    /**
     * Data provider.
     *
     * @return Test data.
     */
    @DataProvider(name = "isValidValueTest")
    public Object[][] isValidValueDataProvider() {
        return new Object[][]{
            {Value.ofNull()},
            {Value.of(true)},
            {Value.of(false)},
            {Value.of(10)},
            {Value.of(new BigDecimal("3.14"))},
            {Value.of("lorem ipsum")},
            {Value.of(Instant.ofEpochMilli(123000))},
            {Value.of(new Hash("8d5f3c77e94a0cad3a32340d342135f43dbb7cbb"))},
            {Value.of(new Guid("8d5f3c77e94a0cad3a32340d342135f4"))},
            {Value.of(array(0xCA, 0xFE, 0xBA, 0xBE))}
        };
    }

    /**
     * Test.
     *
     * @param value test data.
     */
    @Test(dataProvider = "isValidValueTest")
    public void isValidValueTest(Value value) {
        Schema schema = Schema.of("test", ImmutableMap.of("key", value));

        JsonObject json = Json.createObjectBuilder()
                .add("key", ValueWriting.writeValue(value, schema))
                .build();

        assertThat(isValid(json, schema)).as(value.type().name().toLowerCase()).isTrue();
    }

    /**
     * Test.
     */
    @Test
    public void isValidStagingInfoTest() {
        assertThat(isValid(STAGING_INFO_JSON, StagingInfo.class)).isTrue();
        assertThat(isValid(REVISION_TREE_JSON, StagingInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidRevisionTest() {
        REVISIONS_JSON.forEach(json -> {
            assertThat(isValid(json, Revision.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, Revision.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidRevisionTreeTest() {
        assertThat(isValid(REVISION_TREE_JSON, RevisionTree.class)).isTrue();
        assertThat(isValid(REVISIONS_JSON.get(0), RevisionTree.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidContentInfoTest() {
        assertThat(isValid(CONTENT_INFO_JSON, ContentInfo.class)).isTrue();
        assertThat(isValid(REVISIONS_JSON.get(0), ContentInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidCommandResultTest() {
        COMMAND_RESULTS_JSON.forEach(json -> {
            assertThat(isValid(json, CommandResult.class)).isTrue();
        });
        assertThat(isValid(REVISIONS_JSON.get(0), CommandResult.class)).isFalse();
        assertThat(isValid(REVISION_TREE_JSON, CommandResult.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidEventTest() {
        EVENTS_ARRAY.getValuesAs(JsonObject.class).forEach(json -> {
            assertThat(isValid(json, Event.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, Event.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidIndexEntryTest() {
        INDEX_ENTRIES_ARRAY.getValuesAs(JsonObject.class).forEach(json -> {
            assertThat(isValid(json, IndexEntry.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, IndexEntry.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidRepositoryInfoTest() {
        REPOSITORY_INFOS_JSON.forEach(json -> {
            assertThat(isValid(json, RepositoryInfo.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, RepositoryInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidReplicationInfoTest() {
        REPLICATION_INFOS_JSON.forEach(json -> {
            assertThat(isValid(json, ReplicationInfo.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, ReplicationInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidNodeDefTest() {
        NODE_DEFS_ARRAY.getValuesAs(JsonObject.class).forEach(json -> {
            assertThat(isValid(json, NodeDef.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, NodeDef.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidNodeInfoTest() {
        NODE_INFOS_ARRAY.getValuesAs(JsonObject.class).forEach(json -> {
            assertThat(isValid(json, NodeInfo.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, NodeInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidRemoteInfoTest() {
        REMOTE_INFOS_ARRAY.getValuesAs(JsonObject.class).forEach(json -> {
            assertThat(isValid(json, RemoteInfo.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, RemoteInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidNodeExceptionTest() {
        NODE_EXCEPTIONS_ARRAY.getValuesAs(JsonObject.class).forEach(json -> {
            assertThat(isValid(json, NodeException.class)).isTrue();
        });
        assertThat(isValid(REVISION_TREE_JSON, NodeException.class)).isFalse();
    }
}
