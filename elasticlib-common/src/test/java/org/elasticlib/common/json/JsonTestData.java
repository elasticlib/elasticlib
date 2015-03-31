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

import java.util.ArrayList;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;
import static org.elasticlib.common.TestUtil.readJsonArray;
import static org.elasticlib.common.TestUtil.readJsonObject;

final class JsonTestData {

    public static final JsonObject DIGEST_JSON;
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
    public static final JsonArray REMOTE_INFOS_ARRAY;
    public static final JsonArray NODE_EXCEPTIONS_ARRAY;

    static {
        Class<?> clazz = JsonTestData.class;

        DIGEST_JSON = readJsonObject(clazz, "digestTest.json");

        STAGING_INFO_JSON = readJsonObject(clazz, "stagingInfoTest.json");

        REVISIONS_JSON.add(readJsonObject(clazz, "revisionTest0.json"));
        REVISIONS_JSON.add(readJsonObject(clazz, "revisionTest1.json"));

        REVISION_TREE_JSON = readJsonObject(clazz, "revisionTreeTest.json");

        CONTENT_INFO_JSON = readJsonObject(clazz, "contentInfoTest.json");

        EVENTS_ARRAY = readJsonArray(clazz, "eventsTest.json");

        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResultTest1.json"));
        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResultTest2.json"));

        INDEX_ENTRIES_ARRAY = readJsonArray(clazz, "indexEntriesTest.json");

        REPOSITORY_DEFS_ARRAY = readJsonArray(clazz, "repositoryDefsTest.json");
        REPLICATION_DEFS_ARRAY = readJsonArray(clazz, "replicationDefsTest.json");

        REPOSITORY_INFOS_JSON.add(readJsonObject(clazz, "repositoryInfoTest0.json"));
        REPOSITORY_INFOS_JSON.add(readJsonObject(clazz, "repositoryInfoTest1.json"));

        REPLICATION_INFOS_JSON.add(readJsonObject(clazz, "replicationInfoTest0.json"));
        REPLICATION_INFOS_JSON.add(readJsonObject(clazz, "replicationInfoTest1.json"));

        NODE_DEFS_ARRAY = readJsonArray(clazz, "nodeDefsTest.json");
        NODE_INFOS_ARRAY = readJsonArray(clazz, "nodeInfosTest.json");
        REMOTE_INFOS_ARRAY = readJsonArray(clazz, "remoteInfosTest.json");

        NODE_EXCEPTIONS_ARRAY = readJsonArray(clazz, "nodeExceptionsTest.json");
    }

    private JsonTestData() {
    }
}
