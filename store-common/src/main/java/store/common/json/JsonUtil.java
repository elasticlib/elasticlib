package store.common.json;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import static javax.json.JsonValue.ValueType.ARRAY;
import static javax.json.JsonValue.ValueType.FALSE;
import static javax.json.JsonValue.ValueType.NUMBER;
import static javax.json.JsonValue.ValueType.OBJECT;
import static javax.json.JsonValue.ValueType.STRING;
import static javax.json.JsonValue.ValueType.TRUE;
import store.common.Config;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.ContentInfoTree.ContentInfoTreeBuilder;
import store.common.Event;
import store.common.Event.EventBuilder;
import store.common.Hash;
import store.common.Operation;
import static store.common.json.ValueReading.readMap;
import static store.common.json.ValueWriting.writeMap;
import store.common.json.schema.Schema;

/**
 * Utils for reading and writing JSON.
 */
public final class JsonUtil {

    private static final String HASH = "hash";
    private static final String HEAD = "head";
    private static final String REV = "rev";
    private static final String REVS = "revs";
    private static final String PARENTS = "parents";
    private static final String DELETED = "deleted";
    private static final String LENGTH = "length";
    private static final String SCHEMA = "schema";
    private static final String METADATA = "metadata";
    private static final String SEQ = "seq";
    private static final String TIMESTAMP = "timestamp";
    private static final String OPERATION = "operation";
    private static final String REPOSITORIES = "repositories";
    private static final String SYNC = "sync";

    private JsonUtil() {
    }

    /**
     * Checks if supplied JSON object has a string getCode associated to supplied key.
     *
     * @param json A JSON object.
     * @param key A key.
     * @return <tt>true</tt> if supplied JSON object has a string getCode associated to supplied key.
     */
    public static boolean hasStringValue(JsonObject json, String key) {
        return hasValue(json, key, STRING);
    }

    /**
     * Checks if supplied JSON object has a boolean getCode associated to supplied key.
     *
     * @param json A JSON object.
     * @param key A key.
     * @return <tt>true</tt> if supplied JSON object has a boolean getCode associated to supplied key.
     */
    public static boolean hasBooleanValue(JsonObject json, String key) {
        return hasValue(json, key, TRUE, FALSE);
    }

    private static boolean hasValue(JsonObject json, String key, javax.json.JsonValue.ValueType... types) {
        if (!json.containsKey(key)) {
            return false;
        }
        for (javax.json.JsonValue.ValueType type : types) {
            if (json.get(key).getValueType() == type) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if supplied JSON object is a valid {@link ContentInfo}.
     *
     * @param json A JSON object.
     * @return <tt>true</tt> if a ContentInfo can be read from supplied JSON object.
     */
    public static boolean isContentInfo(JsonObject json) {
        return hasValue(json, HASH, STRING) &&
                hasValue(json, LENGTH, NUMBER) &&
                hasValue(json, REV, STRING) &&
                hasValue(json, PARENTS, ARRAY) &&
                hasValue(json, SCHEMA, OBJECT) &&
                hasValue(json, METADATA, OBJECT);
    }

    /**
     * Checks if supplied JSON object is a valid {@link ContentInfoTree}.
     *
     * @param json A JSON object.
     * @return <tt>true</tt> if a ContentInfoTree can be read from supplied JSON object.
     */
    public static boolean isContentInfoTree(JsonObject json) {
        if (!hasValue(json, HASH, STRING) ||
                !hasValue(json, LENGTH, NUMBER) ||
                !hasValue(json, REVS, ARRAY)) {
            return false;
        }
        for (JsonValue value : json.getJsonArray(REVS)) {
            if (value.getValueType() != OBJECT) {
                return false;
            }
            JsonObject obj = (JsonObject) value;
            if (!hasValue(obj, REV, STRING) ||
                    !hasValue(obj, PARENTS, ARRAY) ||
                    !hasValue(obj, SCHEMA, OBJECT) ||
                    !hasValue(obj, METADATA, OBJECT)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Writes supplied list fromCode {@link Hash} to a JSON array.
     *
     * @param hashes A list fromCode hashes.
     * @return A JSON array.
     */
    public static JsonArray writeHashes(List<Hash> hashes) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (Hash hash : hashes) {
            builder.add(hash.encode());
        }
        return builder.build();
    }

    /**
     * Reads a list fromCode {@link Hash} from supplied JSON array.
     *
     * @param json A JSON array.
     * @return A list fromCode hashes.
     */
    public static List<Hash> readHashes(JsonArray json) {
        List<Hash> list = new ArrayList<>();
        for (JsonString value : json.getValuesAs(JsonString.class)) {
            list.add(new Hash(value.getString()));
        }
        return list;
    }

    /**
     * Writes supplied list fromCode {@link ContentInfo} to a JSON array.
     *
     * @param contentInfos A list fromCode content infos.
     * @return A JSON array.
     */
    public static JsonArray writeContentInfos(List<ContentInfo> contentInfos) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (ContentInfo contentInfo : contentInfos) {
            builder.add(writeContentInfo(contentInfo));
        }
        return builder.build();
    }

    /**
     * Reads a list fromCode {@link ContentInfo} from supplied JSON array.
     *
     * @param json A JSON array.
     * @return A list fromCode content infos.
     */
    public static List<ContentInfo> readContentInfos(JsonArray json) {
        List<ContentInfo> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readContentInfo(object));
        }
        return list;
    }

    /**
     * Writes supplied {@link ContentInfo} to a JSON object.
     *
     * @param contentInfo A contentInfo instance.
     * @return A JSON object.
     */
    public static JsonObject writeContentInfo(ContentInfo contentInfo) {
        JsonObjectBuilder builder = createObjectBuilder()
                .add(HASH, contentInfo.getHash().encode())
                .add(LENGTH, contentInfo.getLength());

        return writeContentInfoBase(builder, contentInfo);
    }

    /**
     * Reads a {@link ContentInfo} from supplied JSON object.
     *
     * @param json A JSON object.
     * @return A contentInfo instance.
     */
    public static ContentInfo readContentInfo(JsonObject json) {
        ContentInfoBuilder builder = new ContentInfoBuilder()
                .withHash(new Hash(json.getString(HASH)))
                .withLength(json.getJsonNumber(LENGTH).longValueExact());

        return readContentInfoBase(builder, json);
    }

    /**
     * Writes supplied {@link ContentInfoTree} to a JSON object.
     *
     * @param tree A contentInfoTree instance.
     * @return A JSON object.
     */
    public static JsonObject writeContentInfoTree(ContentInfoTree tree) {
        List<ContentInfo> infos = tree.list();
        JsonArrayBuilder revs = createArrayBuilder();
        for (ContentInfo info : infos) {
            revs.add(writeContentInfoBase(createObjectBuilder(), info));
        }
        return createObjectBuilder()
                .add(HASH, tree.getHash().encode())
                .add(LENGTH, infos.get(0).getLength())
                .add(REVS, revs)
                .build();
    }

    /**
     * Reads a {@link ContentInfoTree} from supplied JSON object.
     *
     * @param json A JSON object.
     * @return A contentInfoTree instance.
     */
    public static ContentInfoTree readContentInfoTree(JsonObject json) {
        List<ContentInfo> infos = new ArrayList<>(json.getJsonArray(REVS).size());
        Hash hash = new Hash(json.getString(HASH));
        long length = json.getJsonNumber(LENGTH).longValueExact();

        for (JsonObject rev : json.getJsonArray(REVS).getValuesAs(JsonObject.class)) {
            ContentInfoBuilder builder = new ContentInfoBuilder()
                    .withHash(hash)
                    .withLength(length);

            infos.add(readContentInfoBase(builder, rev));
        }
        return new ContentInfoTreeBuilder()
                .addAll(infos)
                .build();
    }

    private static JsonObject writeContentInfoBase(JsonObjectBuilder builder, ContentInfo contentInfo) {
        builder.add(REV, contentInfo.getRev().encode());
        JsonArrayBuilder parents = createArrayBuilder();
        for (Hash item : contentInfo.getParents()) {
            parents.add(item.encode());
        }
        builder.add(PARENTS, parents);
        if (contentInfo.isDeleted()) {
            builder.add(DELETED, true);
        }
        return builder
                .add(SCHEMA, Schema.of(METADATA, contentInfo.getMetadata()).write())
                .add(METADATA, writeMap(contentInfo.getMetadata()))
                .build();
    }

    private static ContentInfo readContentInfoBase(ContentInfoBuilder builder, JsonObject json) {
        for (JsonString value : json.getJsonArray(PARENTS).getValuesAs(JsonString.class)) {
            builder.withParent(new Hash(value.getString()));
        }
        if (json.containsKey(DELETED)) {
            builder.withDeleted(json.getBoolean(DELETED));
        }
        Schema schema = Schema.read(json.getJsonObject(SCHEMA));
        return builder
                .withMetadata(readMap(json.getJsonObject(METADATA), schema))
                .build(new Hash(json.getString(REV)));
    }

    /**
     * Writes supplied {@link Config} to a JSON object.
     *
     * @param config A config instance.
     * @return A JSON object.
     */
    public static JsonObject writeConfig(Config config) {
        return createObjectBuilder()
                .add(REPOSITORIES, writePaths(config.getRepositories()))
                .add(SYNC, writeSync(config))
                .build();
    }

    private static JsonArrayBuilder writePaths(List<Path> paths) {
        JsonArrayBuilder array = createArrayBuilder();
        for (Path path : paths) {
            array.add(path.toString());
        }
        return array;
    }

    private static JsonObjectBuilder writeSync(Config config) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Path sourcePath : config.getRepositories()) {
            String source = sourcePath.getFileName().toString();
            Set<String> destinations = config.getSync(source);
            if (!destinations.isEmpty()) {
                JsonArrayBuilder array = createArrayBuilder();
                for (String destination : destinations) {
                    array.add(destination);
                }
                json.add(source, array);
            }
        }
        return json;
    }

    /**
     * Reads a {@link Config} from supplied JSON object.
     *
     * @param json A JSON object.
     * @return A Config instance.
     */
    public static Config readConfig(JsonObject json) {
        return new Config(readPaths(json.getJsonArray(REPOSITORIES)),
                          readSync(json.getJsonObject(SYNC)));
    }

    private static List<Path> readPaths(JsonArray array) {
        List<Path> paths = new ArrayList<>();
        for (JsonString key : array.getValuesAs(JsonString.class)) {
            paths.add(Paths.get(key.getString()));
        }
        return paths;
    }

    private static Map<String, Set<String>> readSync(JsonObject json) {
        Map<String, Set<String>> map = new LinkedHashMap<>();
        for (String key : json.keySet()) {
            map.put(key, readStrings(json.getJsonArray(key)));
        }
        return map;
    }

    private static Set<String> readStrings(JsonArray array) {
        Set<String> strings = new LinkedHashSet<>();
        for (JsonString object : array.getValuesAs(JsonString.class)) {
            strings.add(object.getString());
        }
        return strings;
    }

    /**
     * Writes supplied list fromCode {@link Event} to a JSON array.
     *
     * @param events A list fromCode events.
     * @return A JSON array.
     */
    public static JsonArray writeEvents(List<Event> events) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (Event event : events) {
            builder.add(writeEvent(event));
        }
        return builder.build();
    }

    /**
     * Reads a list fromCode {@link Event} from supplied JSON array.
     *
     * @param json A JSON array.
     * @return A list fromCode events.
     */
    public static List<Event> readEvents(JsonArray json) {
        List<Event> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readEvent(object));
        }
        return list;
    }

    private static JsonObjectBuilder writeEvent(Event event) {
        JsonArrayBuilder head = createArrayBuilder();
        for (Hash item : event.getHead()) {
            head.add(item.encode());
        }
        return createObjectBuilder()
                .add(SEQ, event.getSeq())
                .add(HASH, event.getHash().encode())
                .add(HEAD, head)
                .add(TIMESTAMP, event.getTimestamp().getTime())
                .add(OPERATION, event.getOperation().toString());
    }

    private static Event readEvent(JsonObject json) {
        SortedSet<Hash> head = new TreeSet<>();
        for (JsonString value : json.getJsonArray(HEAD).getValuesAs(JsonString.class)) {
            head.add(new Hash(value.getString()));
        }
        return new EventBuilder()
                .withSeq(json.getJsonNumber(SEQ).longValue())
                .withHash(new Hash(json.getString(HASH)))
                .withHead(head)
                .withTimestamp(new Date(json.getJsonNumber(TIMESTAMP).longValue()))
                .withOperation(Operation.fromString(json.getString(OPERATION)))
                .build();
    }
}
