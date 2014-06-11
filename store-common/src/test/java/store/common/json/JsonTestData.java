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
    public static final JsonArray EVENTS_ARRAY;
    public static final List<JsonObject> COMMAND_RESULTS_JSON = new ArrayList<>();
    public static final JsonArray INDEX_ENTRIES_ARRAY;
    public static final JsonArray REPOSITORY_DEFS_ARRAY;
    public static final JsonArray REPLICATION_DEFS_ARRAY;
    public static final List<JsonObject> REPOSITORY_INFOS_JSON = new ArrayList<>();
    public static final List<JsonObject> REPLICATION_INFOS_JSON = new ArrayList<>();

    static {
        Class<?> clazz = JsonTestData.class;

        CONTENT_INFOS_JSON.add(readJsonObject(clazz, "contentInfo0.json"));
        CONTENT_INFOS_JSON.add(readJsonObject(clazz, "contentInfo1.json"));

        CONTENT_INFO_TREE_JSON = readJsonObject(clazz, "contentInfoTree.json");

        EVENTS_ARRAY = readJsonArray(clazz, "events.json");

        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResult1.json"));
        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResult2.json"));

        INDEX_ENTRIES_ARRAY = readJsonArray(clazz, "indexEntries.json");

        REPOSITORY_DEFS_ARRAY = readJsonArray(clazz, "repositoryDefs.json");
        REPLICATION_DEFS_ARRAY = readJsonArray(clazz, "replicationDefs.json");

        REPOSITORY_INFOS_JSON.add(readJsonObject(clazz, "repositoryInfo0.json"));
        REPOSITORY_INFOS_JSON.add(readJsonObject(clazz, "repositoryInfo1.json"));

        REPLICATION_INFOS_JSON.add(readJsonObject(clazz, "replicationInfo0.json"));
        REPLICATION_INFOS_JSON.add(readJsonObject(clazz, "replicationInfo1.json"));
    }

    private JsonTestData() {
    }
}
