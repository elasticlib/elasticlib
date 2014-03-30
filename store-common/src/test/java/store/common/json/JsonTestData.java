package store.common.json;

import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;
import javax.json.JsonArray;
import javax.json.JsonObject;
import store.common.CommandResult;
import store.common.Config;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Event.EventBuilder;
import store.common.Hash;
import store.common.IndexEntry;
import store.common.Operation;
import static store.common.TestUtil.readJsonArray;
import static store.common.TestUtil.readJsonObject;
import store.common.value.Value;

final class JsonTestData {

    private static final String[] HASHES;
    private static final String[] REVS;
    public static final List<ContentInfo> CONTENT_INFOS = new ArrayList<>();
    public static final List<JsonObject> CONTENT_INFOS_JSON = new ArrayList<>();
    public static final ContentInfoTree CONTENT_INFO_TREE;
    public static final JsonObject CONTENT_INFO_TREE_JSON;
    public static final Config CONFIG;
    public static final JsonObject CONFIG_JSON;
    public static final List<Event> EVENTS = new ArrayList<>();
    public static final JsonArray EVENTS_ARRAY;
    public static final List<CommandResult> COMMAND_RESULTS = new ArrayList<>();
    public static final List<JsonObject> COMMAND_RESULTS_JSON = new ArrayList<>();
    public static final List<IndexEntry> INDEX_ENTRIES = new ArrayList<>();
    public static final JsonArray INDEX_ENTRIES_ARRAY;

    static {
        Class<?> clazz = JsonTestData.class;

        HASHES = new String[]{"8d5f3c77e94a0cad3a32340d342135f43dbb7cbb",
                              "0827c43f0aad546501f99b11f0bd44be42d68870",
                              "39819150ee99549a8c0a59782169bb3be65b46a4"};

        REVS = new String[]{"0d99dd9895a2a1c485e0c75f79f92cc14457bb62",
                            "a0b87ac4b04a0bed394517d0b01792635531aa42"};

        CONTENT_INFOS.add(new ContentInfoBuilder()
                .withHash(new Hash(HASHES[0]))
                .withLength(10)
                .with("text", Value.of("ipsum lorem"))
                .build(new Hash(REVS[0])));

        CONTENT_INFOS.add(new ContentInfoBuilder()
                .withHash(new Hash(HASHES[1]))
                .withLength(120)
                .withParent(new Hash(REVS[0]))
                .build(new Hash(REVS[1])));

        CONTENT_INFOS_JSON.add(readJsonObject(clazz, "contentInfo0.json"));
        CONTENT_INFOS_JSON.add(readJsonObject(clazz, "contentInfo1.json"));

        CONTENT_INFO_TREE = new ContentInfoTree.ContentInfoTreeBuilder()
                .add(CONTENT_INFOS.get(0))
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
        CONFIG.addReplication("primary", "secondary");

        CONFIG_JSON = readJsonObject(clazz, "config.json");

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

    private JsonTestData() {
    }
}
