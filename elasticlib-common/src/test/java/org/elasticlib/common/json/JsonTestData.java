package org.elasticlib.common.json;

import java.util.ArrayList;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import static org.elasticlib.common.TestUtil.readJsonArray;
import static org.elasticlib.common.TestUtil.readJsonObject;

final class JsonTestData {

    public static final JsonObject STAGING_INFO_JSON;
    public static final List<JsonObject> REVISIONS_JSON = new ArrayList<>();
    public static final JsonObject REVISION_TREE_JSON;
    public static final JsonObject CONTENT_INFO_JSON;
    public static final JsonArray EVENTS_ARRAY;
    public static final List<JsonObject> COMMAND_RESULTS_JSON = new ArrayList<>();
    public static final JsonArray INDEX_ENTRIES_ARRAY;
    public static final JsonArray REPOSITORY_DEFS_ARRAY;
    public static final JsonArray REPLICATION_DEFS_ARRAY;
    public static final List<JsonObject> REPOSITORY_INFOS_JSON = new ArrayList<>();
    public static final List<JsonObject> REPLICATION_INFOS_JSON = new ArrayList<>();
    public static final JsonArray NODE_DEFS_ARRAY;
    public static final JsonArray NODE_INFOS_ARRAY;
    public static final JsonArray NODE_EXCEPTIONS_ARRAY;

    static {
        Class<?> clazz = JsonTestData.class;

        STAGING_INFO_JSON = readJsonObject(clazz, "stagingInfo.json");

        REVISIONS_JSON.add(readJsonObject(clazz, "revision0.json"));
        REVISIONS_JSON.add(readJsonObject(clazz, "revision1.json"));

        REVISION_TREE_JSON = readJsonObject(clazz, "revisionTree.json");

        CONTENT_INFO_JSON = readJsonObject(clazz, "contentInfo.json");

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

        NODE_DEFS_ARRAY = readJsonArray(clazz, "nodeDefs.json");
        NODE_INFOS_ARRAY = readJsonArray(clazz, "nodeInfos.json");

        NODE_EXCEPTIONS_ARRAY = readJsonArray(clazz, "nodeExceptions.json");
    }

    private JsonTestData() {
    }
}