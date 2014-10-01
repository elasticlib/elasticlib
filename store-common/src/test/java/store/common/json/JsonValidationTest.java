package store.common.json;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import javax.json.Json;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import static org.fest.assertions.api.Assertions.assertThat;
import org.joda.time.Instant;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static store.common.TestUtil.array;
import store.common.exception.NodeException;
import store.common.hash.Guid;
import store.common.hash.Hash;
import static store.common.json.JsonTestData.COMMAND_RESULTS_JSON;
import static store.common.json.JsonTestData.CONTENT_INFOS_JSON;
import static store.common.json.JsonTestData.CONTENT_INFO_TREE_JSON;
import static store.common.json.JsonTestData.EVENTS_ARRAY;
import static store.common.json.JsonTestData.INDEX_ENTRIES_ARRAY;
import static store.common.json.JsonTestData.NODE_DEFS_ARRAY;
import static store.common.json.JsonTestData.NODE_EXCEPTIONS_ARRAY;
import static store.common.json.JsonTestData.NODE_INFOS_ARRAY;
import static store.common.json.JsonTestData.REPLICATION_INFOS_JSON;
import static store.common.json.JsonTestData.REPOSITORY_INFOS_JSON;
import static store.common.json.JsonValidation.hasArrayValue;
import static store.common.json.JsonValidation.hasBooleanValue;
import static store.common.json.JsonValidation.hasStringValue;
import static store.common.json.JsonValidation.isValid;
import store.common.json.schema.Schema;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentInfoTree;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.common.model.ReplicationInfo;
import store.common.model.RepositoryInfo;
import store.common.value.Value;

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
            {Value.of(new Instant(123000))},
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
    public void isValidContentInfoTest() {
        for (JsonObject json : CONTENT_INFOS_JSON) {
            assertThat(isValid(json, ContentInfo.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, ContentInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidContentInfoTreeTest() {
        assertThat(isValid(CONTENT_INFO_TREE_JSON, ContentInfoTree.class)).isTrue();
        assertThat(isValid(CONTENT_INFOS_JSON.get(0), ContentInfoTree.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidCommandResultTest() {
        for (JsonObject json : COMMAND_RESULTS_JSON) {
            assertThat(isValid(json, CommandResult.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFOS_JSON.get(0), CommandResult.class)).isFalse();
        assertThat(isValid(CONTENT_INFO_TREE_JSON, CommandResult.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidEventTest() {
        for (JsonObject json : EVENTS_ARRAY.getValuesAs(JsonObject.class)) {
            assertThat(isValid(json, Event.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, Event.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidIndexEntryTest() {
        for (JsonObject json : INDEX_ENTRIES_ARRAY.getValuesAs(JsonObject.class)) {
            assertThat(isValid(json, IndexEntry.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, IndexEntry.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidRepositoryInfoTest() {
        for (JsonObject json : REPOSITORY_INFOS_JSON) {
            assertThat(isValid(json, RepositoryInfo.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, RepositoryInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidReplicationInfoTest() {
        for (JsonObject json : REPLICATION_INFOS_JSON) {
            assertThat(isValid(json, ReplicationInfo.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, ReplicationInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidNodeDefTest() {
        for (JsonObject json : NODE_DEFS_ARRAY.getValuesAs(JsonObject.class)) {
            assertThat(isValid(json, NodeDef.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, NodeDef.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidNodeInfoTest() {
        for (JsonObject json : NODE_INFOS_ARRAY.getValuesAs(JsonObject.class)) {
            assertThat(isValid(json, NodeInfo.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, NodeInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidNodeExceptionTest() {
        for (JsonObject json : NODE_EXCEPTIONS_ARRAY.getValuesAs(JsonObject.class)) {
            assertThat(isValid(json, NodeException.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, NodeException.class)).isFalse();
    }
}
