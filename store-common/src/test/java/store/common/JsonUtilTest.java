package store.common;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonObject;
import static org.fest.assertions.api.Assertions.assertThat;
import org.testng.annotations.Test;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.Event.EventBuilder;
import static store.common.JsonUtil.hasBooleanValue;
import static store.common.JsonUtil.hasStringValue;
import static store.common.JsonUtil.readConfig;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.readContentInfos;
import static store.common.JsonUtil.readHashes;
import static store.common.JsonUtil.writeConfig;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.JsonUtil.writeContentInfos;
import static store.common.JsonUtil.writeEvents;
import static store.common.JsonUtil.writeHashes;
import store.common.value.Value;

/**
 * Unit tests.
 */
public class JsonUtilTest {

    private static final String[] HASHES;
    private static final String[] REV;
    private static final JsonArray HASHES_ARRAY;
    private static final Map<String, ContentInfo> CONTENT_INFOS = new HashMap<>();
    private static final Map<String, JsonObject> CONTENT_INFOS_JSON = new HashMap<>();
    private static final Config CONFIG;
    private static final JsonObject CONFIG_JSON;
    private static final List<Event> EVENTS = new ArrayList<>();
    private static final JsonArray EVENTS_ARRAY;

    static {
        HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                              "0827c43f0aad546501f99b11f0bd44be42d68870",
                              "39819150ee99549a8c0a59782169bb3be65b46a4"};

        REV = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                           "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        HASHES_ARRAY = createArrayBuilder()
                .add(HASHES[0])
                .add(HASHES[1])
                .add(HASHES[2])
                .build();

        CONTENT_INFOS.put(HASHES[0], new ContentInfoBuilder()
                .withHash(new Hash(HASHES[0]))
                .withRev(new Hash(REV[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build());

        CONTENT_INFOS_JSON.put(HASHES[0], createObjectBuilder()
                .add("hash", HASHES[0])
                .add("rev", REV[0])
                .add("parents", createArrayBuilder())
                .add("length", 10)
                .add("metadata", createObjectBuilder()
                .add("text", createObjectBuilder()
                .add("type", "STRING")
                .add("value", "ipsum lorem")))
                .build());

        CONTENT_INFOS.put(HASHES[1], new ContentInfoBuilder()
                .withHash(new Hash(HASHES[1]))
                .withRev(new Hash(REV[1]))
                .withParent(new Hash(REV[0]))
                .withLength(120)
                .build());

        CONTENT_INFOS_JSON.put(HASHES[1], createObjectBuilder()
                .add("hash", HASHES[1])
                .add("rev", REV[1])
                .add("parents", createArrayBuilder().add(REV[0]))
                .add("length", 120)
                .add("metadata", createObjectBuilder())
                .build());

        CONFIG = new Config();
        CONFIG.addRepository(Paths.get("/repo/primary"));
        CONFIG.addRepository(Paths.get("/repo/secondary"));
        CONFIG.sync("primary", "secondary");

        CONFIG_JSON = createObjectBuilder()
                .add("repositories", createArrayBuilder().add("/repo/primary").add("/repo/secondary"))
                .add("sync", createObjectBuilder().add("primary", createArrayBuilder().add("secondary")))
                .build();

        EVENTS.add(new EventBuilder()
                .withSeq(0)
                .withHash(new Hash(HASHES[0]))
                .withTimestamp(new Date(0))
                .withOperation(Operation.PUT)
                .build());

        EVENTS.add(new EventBuilder()
                .withSeq(1)
                .withHash(new Hash(HASHES[1]))
                .withTimestamp(new Date(123456))
                .withOperation(Operation.DELETE)
                .build());

        EVENTS_ARRAY = createArrayBuilder()
                .add(createObjectBuilder()
                .add("seq", 0)
                .add("hash", HASHES[0])
                .add("timestamp", 0)
                .add("operation", "PUT"))
                .add(createObjectBuilder()
                .add("seq", 1)
                .add("hash", HASHES[1])
                .add("timestamp", 123456)
                .add("operation", "DELETE"))
                .build();
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
    public void writeHashesTest() {
        List<Hash> hashes = asList(new Hash(HASHES[0]),
                                   new Hash(HASHES[1]),
                                   new Hash(HASHES[2]));

        assertThat(writeHashes(hashes)).isEqualTo(HASHES_ARRAY);
    }

    /**
     * Test.
     */
    @Test
    public void readHashesTest() {
        assertThat(readHashes(HASHES_ARRAY)).containsExactly(new Hash(HASHES[0]),
                                                             new Hash(HASHES[1]),
                                                             new Hash(HASHES[2]));
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
        assertThat(JsonUtil.readEvents(EVENTS_ARRAY)).isEqualTo(EVENTS);
    }
}
