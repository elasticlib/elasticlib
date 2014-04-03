package store.common.yaml;

import java.util.ArrayList;
import java.util.List;
import static store.common.TestUtil.readString;

final class YamlTestData {

    public static final List<String> CONTENT_INFOS_YAML = new ArrayList<>();
    public static final String CONTENT_INFO_TREE_YAML;
    public static final List<String> EVENTS_YAML = new ArrayList<>();
    public static final List<String> COMMAND_RESULTS_YAML = new ArrayList<>();
    public static final List<String> INDEX_ENTRIES_YAML = new ArrayList<>();

    static {
        Class<?> clazz = YamlTestData.class;

        CONTENT_INFOS_YAML.add(readString(clazz, "contentInfo0.yml"));
        CONTENT_INFOS_YAML.add(readString(clazz, "contentInfo1.yml"));

        CONTENT_INFO_TREE_YAML = readString(clazz, "contentInfoTree.yml");

        EVENTS_YAML.add(readString(clazz, "event0.yml"));
        EVENTS_YAML.add(readString(clazz, "event1.yml"));

        COMMAND_RESULTS_YAML.add(readString(clazz, "commandResult1.yml"));
        COMMAND_RESULTS_YAML.add(readString(clazz, "commandResult2.yml"));

        INDEX_ENTRIES_YAML.add(readString(clazz, "indexEntry0.yml"));
        INDEX_ENTRIES_YAML.add(readString(clazz, "indexEntry1.yml"));
    }

    private YamlTestData() {
    }
}
