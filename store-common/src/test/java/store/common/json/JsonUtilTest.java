package store.common.json;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonObject;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.CommandResult;
import store.common.Config;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.Event;
import store.common.Event.EventBuilder;
import store.common.Hash;
import store.common.IndexEntry;
import store.common.Operation;
import static store.common.TestUtil.readJsonArray;
import static store.common.TestUtil.readJsonObject;
import static store.common.json.JsonUtil.hasBooleanValue;
import static store.common.json.JsonUtil.hasStringValue;
import static store.common.json.JsonUtil.isContentInfo;
import static store.common.json.JsonUtil.isContentInfoTree;
import static store.common.json.JsonUtil.readCommandResult;
import static store.common.json.JsonUtil.readConfig;
import static store.common.json.JsonUtil.readContentInfo;
import static store.common.json.JsonUtil.readContentInfoTree;
import static store.common.json.JsonUtil.readContentInfos;
import static store.common.json.JsonUtil.readEvents;
import static store.common.json.JsonUtil.readIndexEntries;
import static store.common.json.JsonUtil.writeCommandResult;
import static store.common.json.JsonUtil.writeConfig;
import static store.common.json.JsonUtil.writeContentInfo;
import static store.common.json.JsonUtil.writeContentInfoTree;
import static store.common.json.JsonUtil.writeContentInfos;
import static store.common.json.JsonUtil.writeEvents;
import static store.common.json.JsonUtil.writeIndexEntries;
import store.common.value.Value;

/**
 * Unit tests.
 */
public class JsonUtilTest {

    private static final String[] HASHES;
    private static final String[] REVS;
    private static final Map<String, ContentInfo> CONTENT_INFOS = new HashMap<>();
    private static final Map<String, JsonObject> CONTENT_INFOS_JSON = new HashMap<>();
    private static final ContentInfoTree CONTENT_INFO_TREE;
    private static final JsonObject CONTENT_INFO_TREE_JSON;
    private static final Config CONFIG;
    private static final JsonObject CONFIG_JSON;
    private static final List<Event> EVENTS = new ArrayList<>();
    private static final JsonArray EVENTS_ARRAY;
    private static final List<CommandResult> COMMAND_RESULTS = new ArrayList<>();
    private static final List<JsonObject> COMMAND_RESULTS_JSON = new ArrayList<>();
    private static final List<IndexEntry> INDEX_ENTRIES = new ArrayList<>();
    private static final JsonArray INDEX_ENTRIES_ARRAY;

    static {
        Class<?> clazz = JsonUtilTest.class;

        HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                              "0827c43f0aad546501f99b11f0bd44be42d68870",
                              "39819150ee99549a8c0a59782169bb3be65b46a4"};

        REVS = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                            "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        CONTENT_INFOS.put(HASHES[0], new ContentInfoBuilder()
                .withHash(new Hash(HASHES[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build(new Hash(REVS[0])));

        CONTENT_INFOS.put(HASHES[1], new ContentInfoBuilder()
                .withHash(new Hash(HASHES[1]))
                .withLength(120)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])));

        CONTENT_INFOS_JSON.put(HASHES[0], readJsonObject(clazz, "contentInfo0.json"));
        CONTENT_INFOS_JSON.put(HASHES[1], readJsonObject(clazz, "contentInfo1.json"));

        CONTENT_INFO_TREE = new ContentInfoTreeBuilder()
                .add(info(0))
                .add(new ContentInfoBuilder()
                .withHash(new Hash(HASHES[0]))
                .withLength(10)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])))
                .build();

        CONTENT_INFO_TREE_JSON = readJsonObject(clazz, "contentInfoTree.json");

        CONFIG = new Config();
        CONFIG.addRepository(Paths.get("/repo/primary"));
        CONFIG.addRepository(Paths.get("/repo/secondary"));
        CONFIG.sync("primary", "secondary");

        CONFIG_JSON = readJsonObject(JsonUtilTest.class, "config.json");

        EVENTS.add(new EventBuilder()
                .withSeq(0)
                .withHash(new Hash(HASHES[0]))
                .withHead(new TreeSet<>(singleton(new Hash(REVS[0]))))
                .withTimestamp(new Date(0))
                .withOperation(Operation.CREATE)
                .build());

        EVENTS.add(new EventBuilder()
                .withSeq(1)
                .withHash(new Hash(HASHES[1]))
                .withHead(new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1]))))
                .withTimestamp(new Date(123456))
                .withOperation(Operation.DELETE)
                .build());

        EVENTS_ARRAY = readJsonArray(clazz, "events.json");

        COMMAND_RESULTS.add(CommandResult.of(1, Operation.CREATE, new TreeSet<>(singleton(new Hash(REVS[0])))));
        COMMAND_RESULTS.add(CommandResult.noOp(2, new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResult1.json"));
        COMMAND_RESULTS_JSON.add(readJsonObject(clazz, "commandResult2.json"));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[0]),
                                         new TreeSet<>(singleton(new Hash(REVS[0])))));

        INDEX_ENTRIES.add(new IndexEntry(new Hash(HASHES[1]),
                                         new TreeSet<>(asList(new Hash(REVS[0]), new Hash(REVS[1])))));

        INDEX_ENTRIES_ARRAY = readJsonArray(clazz, "indexEntries.json");
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
     * Test.
     */
    @Test
    public void isContentInfoTest() {
        assertThat(isContentInfo(infoJson(0))).isTrue();
        assertThat(isContentInfo(CONTENT_INFO_TREE_JSON)).isFalse();
        assertThat(isContentInfo(CONFIG_JSON)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void isContentInfoTreeTest() {
        assertThat(isContentInfoTree(CONTENT_INFO_TREE_JSON)).isTrue();
        assertThat(isContentInfoTree(infoJson(0))).isFalse();
        assertThat(isContentInfoTree(CONFIG_JSON)).isFalse();
    }

    /**
     * Test.
     */
    @Test
    public void writeContentInfosTest() {
        assertThat(writeContentInfos(asList(info(0),
                                            info(1))))
                .containsExactly(infoJson(0),
                                 infoJson(1));
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfosTest() {
        JsonArray array = createArrayBuilder()
                .add(infoJson(0))
                .add(infoJson(1))
                .build();

        assertThat(readContentInfos(array)).containsExactly(info(0),
                                                            info(1));
    }

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTest() {
        assertThat(writeContentInfo(info(0))).isEqualTo(infoJson(0));
        assertThat(writeContentInfo(info(1))).isEqualTo(infoJson(1));
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfoTest() {
        assertThat(readContentInfo(infoJson(0))).isEqualTo(info(0));
        assertThat(readContentInfo(infoJson(1))).isEqualTo(info(1));
    }

    /**
     * Test.
     */
    @Test
    public void writeContentInfoTreeTest() {
        assertThat(writeContentInfoTree(CONTENT_INFO_TREE)).isEqualTo(CONTENT_INFO_TREE_JSON);
    }

    /**
     * Test.
     */
    @Test
    public void readContentInfoTreeTest() {
        assertThat(readContentInfoTree(CONTENT_INFO_TREE_JSON)).isEqualTo(CONTENT_INFO_TREE);
    }

    private static ContentInfo info(int index) {
        return CONTENT_INFOS.get(HASHES[index]);
    }

    private static JsonObject infoJson(int index) {
        return CONTENT_INFOS_JSON.get(HASHES[index]);
    }

    /**
     * Test.
     */
    @Test
    public void writeConfigTest() {
        assertThat(writeConfig(CONFIG)).isEqualTo(CONFIG_JSON);
    }

    /**
     * Test.
     */
    @Test
    public void readConfigTest() {
        assertThat(readConfig(CONFIG_JSON)).isEqualTo(CONFIG);
    }

    /**
     * Test.
     */
    @Test
    public void writeEventsTest() {
        assertThat(writeEvents(EVENTS)).isEqualTo(EVENTS_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void readEventsTest() {
        assertThat(readEvents(EVENTS_ARRAY)).isEqualTo(EVENTS);
    }

    /**
     * Test.
     */
    @Test
    public void writeCommandResultTest() {
        assertThat(writeCommandResult(COMMAND_RESULTS.get(0))).isEqualTo(COMMAND_RESULTS_JSON.get(0));
        assertThat(writeCommandResult(COMMAND_RESULTS.get(1))).isEqualTo(COMMAND_RESULTS_JSON.get(1));
    }

    /**
     * Test.
     */
    @Test
    public void readCommandResultTest() {
        assertThat(readCommandResult(COMMAND_RESULTS_JSON.get(0))).isEqualTo(COMMAND_RESULTS.get(0));
        assertThat(readCommandResult(COMMAND_RESULTS_JSON.get(1))).isEqualTo(COMMAND_RESULTS.get(1));
    }

    /**
     * Test.
     */
    @Test
    public void writeIndexEntriesTest() {
        assertThat(writeIndexEntries(INDEX_ENTRIES)).isEqualTo(INDEX_ENTRIES_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void readIndexEntriesTest() {
        assertThat(readIndexEntries(INDEX_ENTRIES_ARRAY)).isEqualTo(INDEX_ENTRIES);
    }
}
