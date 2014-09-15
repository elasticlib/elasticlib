package store.common.yaml;

import java.util.ArrayList;
import java.util.List;
import static store.common.TestUtil.readString;

final class YamlTestData {

    public static final List<String> CONTENT_INFOS_YAML = new ArrayList<>();
    public static final String CONTENT_INFO_TREE_YAML;
    public static final String EVENTS_YAML;
    public static final List<String> COMMAND_RESULTS_YAML = new ArrayList<>();
    public static final String INDEX_ENTRIES_YAML;
    public static final String REPOSITORY_DEFS_YAML;
    public static final String REPLICATION_DEFS_YAML;
    public static final List<String> REPOSITORY_INFOS_YAML = new ArrayList<>();
    public static final List<String> REPLICATION_INFOS_YAML = new ArrayList<>();
    public static final String NODE_DEFS_YAML;
    public static final String NODE_INFOS_YAML;

    static {
        Class<?> clazz = YamlTestData.class;

        CONTENT_INFOS_YAML.add(readString(clazz, "contentInfo0.yml"));
        CONTENT_INFOS_YAML.add(readString(clazz, "contentInfo1.yml"));

        CONTENT_INFO_TREE_YAML = readString(clazz, "contentInfoTree.yml");

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
    }

    private YamlTestData() {
    }
}
