package store.common.json;

import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.CommandResult;
import store.common.Config;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import static store.common.json.JsonTestData.*;
import static store.common.json.JsonValidation.hasBooleanValue;
import static store.common.json.JsonValidation.hasStringValue;
import static store.common.json.JsonValidation.isValid;

/**
 * Unit tests.
 */
public class JsonValidationTest {

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
     * Test.
     */
    @Test
    public void isValidContentInfoTest() {
        for (JsonObject json : CONTENT_INFOS_JSON) {
            assertThat(isValid(json, ContentInfo.class)).isTrue();
        }
        assertThat(isValid(CONTENT_INFO_TREE_JSON, ContentInfo.class)).isFalse();
        assertThat(isValid(CONFIG_JSON, ContentInfo.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidContentInfoTreeTest() {
        assertThat(isValid(CONTENT_INFO_TREE_JSON, ContentInfoTree.class)).isTrue();
        assertThat(isValid(CONTENT_INFOS_JSON.get(0), ContentInfoTree.class)).isFalse();
        assertThat(isValid(CONFIG_JSON, ContentInfoTree.class)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isValidConfigTest() {
        assertThat(isValid(CONFIG_JSON, Config.class)).isTrue();
        assertThat(isValid(CONTENT_INFOS_JSON.get(0), Config.class)).isFalse();
        assertThat(isValid(CONTENT_INFO_TREE_JSON, Config.class)).isFalse();
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
        assertThat(isValid(CONFIG_JSON, Event.class)).isFalse();
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
        assertThat(isValid(CONFIG_JSON, IndexEntry.class)).isFalse();
        assertThat(isValid(CONTENT_INFO_TREE_JSON, IndexEntry.class)).isFalse();
    }
}
