package store.common.json;

import java.util.ArrayList;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import static store.common.TestUtil.readJsonArray;
import static store.common.TestUtil.readJsonObject;

final class JsonTestData {

    public static final List<JsonObject> CONTENT_INFOS_JSON = new ArrayList<>();
    public static final JsonObject CONTENT_INFO_TREE_JSON;
    public static final JsonObject CONFIG_JSON;
    public static final JsonArray EVENTS_ARRAY;
    public static final List<JsonObject> COMMAND_RESULTS_JSON = new ArrayList<>();
    public static final JsonArray INDEX_ENTRIES_ARRAY;

    static {
        Class<?> clazz = JsonTestData.class;

        CONTENT_INFOS_JSON.add(readJsonObject(clazz, "contentInfo0.json"));
        CONTENT_INFOS_JSON.add(readJsonObject(clazz, "contentInfo1.json"));

        CONTENT_INFO_TREE_JSON = readJsonObject(clazz, "contentInfoTree.json");

        CONFIG_JSON = readJsonObject(clazz, "config.json");

        EVENTS_ARRAY = readJsonArray(clazz, "events.json");

        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResult1.json"));
        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResult2.json"));

        INDEX_ENTRIES_ARRAY = readJsonArray(clazz, "indexEntries.json");
    }

    private JsonTestData() {
    }
}
