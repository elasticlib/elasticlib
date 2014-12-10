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

import java.util.ArrayList;
import java.util.List;
import static org.elasticlib.common.TestUtil.readString;

final class YamlTestData {

    public static final String STAGING_INFO_YAML;
    public static final List<String> REVISIONS_YAML = new ArrayList<>();
    public static final String REVISION_TREE_YAML;
    public static final String CONTENT_INFO_YAML;
    public static final String EVENTS_YAML;
    public static final List<String> COMMAND_RESULTS_YAML = new ArrayList<>();
    public static final String INDEX_ENTRIES_YAML;
    public static final String REPOSITORY_DEFS_YAML;
    public static final String REPLICATION_DEFS_YAML;
    public static final List<String> REPOSITORY_INFOS_YAML = new ArrayList<>();
    public static final List<String> REPLICATION_INFOS_YAML = new ArrayList<>();
    public static final String NODE_DEFS_YAML;
    public static final String NODE_INFOS_YAML;
    public static final String NODE_EXCEPTIONS_YAML;

    static {
        Class<?> clazz = YamlTestData.class;

        STAGING_INFO_YAML = readString(clazz, "stagingInfo.yml");

        REVISIONS_YAML.add(readString(clazz, "revision0.yml"));
        REVISIONS_YAML.add(readString(clazz, "revision1.yml"));

        REVISION_TREE_YAML = readString(clazz, "revisionTree.yml");

        CONTENT_INFO_YAML = readString(clazz, "contentInfo.yml");

        EVENTS_YAML = readString(clazz, "events.yml");

        COMMAND_RESULTS_YAML.add(readString(clazz, "commandResult1.yml"));
        COMMAND_RESULTS_YAML.add(readString(clazz, "commandResult2.yml"));

        INDEX_ENTRIES_YAML = readString(clazz, "indexEntries.yml");

        REPOSITORY_DEFS_YAML = readString(clazz, "repositoryDefs.yml");
        REPLICATION_DEFS_YAML = readString(clazz, "replicationDefs.yml");

        REPOSITORY_INFOS_YAML.add(readString(clazz, "repositoryInfo0.yml"));
        REPOSITORY_INFOS_YAML.add(readString(clazz, "repositoryInfo1.yml"));

        REPLICATION_INFOS_YAML.add(readString(clazz, "replicationInfo0.yml"));
        REPLICATION_INFOS_YAML.add(readString(clazz, "replicationInfo1.yml"));

        NODE_DEFS_YAML = readString(clazz, "nodeDefs.yml");
        NODE_INFOS_YAML = readString(clazz, "nodeInfos.yml");

        NODE_EXCEPTIONS_YAML = readString(clazz, "nodeExceptions.yml");
    }

    private YamlTestData() {
    }
}
