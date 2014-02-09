package store.common;

import static com.google.common.io.BaseEncoding.base16;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import static javax.json.JsonValue.ValueType.FALSE;
import static javax.json.JsonValue.ValueType.STRING;
import static javax.json.JsonValue.ValueType.TRUE;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.Event.EventBuilder;
import store.common.value.Value;
import store.common.value.ValueType;

/**
 * Utils for reading and writing JSON.
 */
public final class JsonUtil {

    private static final String VALUE = "value";
    private static final String TYPE = "type";
    private static final String HASH = "hash";
    private static final String REV = "rev";
    private static final String PARENTS = "parents";
    private static final String DELETED = "deleted";
    private static final String LENGTH = "length";
    private static final String METADATA = "metadata";
    private static final String SEQ = "seq";
    private static final String TIMESTAMP = "timestamp";
    private static final String OPERATION = "operation";
    private static final String REPOSITORIES = "repositories";
    private static final String SYNC = "sync";

    private JsonUtil() {
    }

    /**
     * Checks if supplied JSON object has a string value associated to supplied key.
     *
     * @param json A JSON object.
     * @param key A key.
     * @return <tt>true</tt> if supplied JSON object has a string value associated to supplied key.
     */
    public static boolean hasStringValue(JsonObject json, String key) {
        if (!json.containsKey(key)) {
            return false;
        }
        return json.get(key).getValueType() == STRING;
    }

    /**
     * Checks if supplied JSON object has a boolean value associated to supplied key.
     *
     * @param json A JSON object.
     * @param key A key.
     * @return <tt>true</tt> if supplied JSON object has a boolean value associated to supplied key.
     */
    public static boolean hasBooleanValue(JsonObject json, String key) {
        if (!json.containsKey(key)) {
            return false;
        }
        switch (json.get(key).getValueType()) {
            case TRUE:
            case FALSE:
                return true;

            default:
                return false;
        }
    }

    /**
     * Writes supplied list of {@link Hash} to a JSON array.
     *
     * @param hashes A list of hashes.
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
     * Reads a list of {@link Hash} from supplied JSON array.
     *
     * @param json A JSON array.
     * @return A list of hashes.
     */
    public static List<Hash> readHashes(JsonArray json) {
        List<Hash> list = new ArrayList<>();
        for (JsonString value : json.getValuesAs(JsonString.class)) {
            list.add(new Hash(value.getString()));
        }
        return list;
    }

    /**
     * Writes supplied list of {@link ContentInfo} to a JSON array.
     *
     * @param contentInfos A list of content infos.
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
     * Reads a list of {@link ContentInfo} from supplied JSON array.
     *
     * @param json A JSON array.
     * @return A list of content infos.
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
                .add(REV, contentInfo.getRev().encode());

        JsonArrayBuilder parents = createArrayBuilder();
        for (Hash item : contentInfo.getParents()) {
            parents.add(item.encode());
        }
        builder.add(PARENTS, parents);
        if (contentInfo.isDeleted()) {
            builder.add(DELETED, true);
        }
        return builder
                .add(LENGTH, contentInfo.getLength())
                .add(METADATA, writeMap(contentInfo.getMetadata()))
                .build();
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
                .withRev(new Hash(json.getString(REV)));

        for (JsonString value : json.getJsonArray(PARENTS).getValuesAs(JsonString.class)) {
            builder.withParent(new Hash(value.getString()));
        }
        if (json.containsKey(DELETED)) {
            builder.withDeleted(json.getBoolean(DELETED));
        }
        return builder
                .withLength(json.getJsonNumber(LENGTH).longValue())
                .withMetadata(readMap(json.getJsonObject(METADATA)))
                .build();
    }

    private static JsonObjectBuilder writeMap(Map<String, Value> map) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Entry<String, Value> entry : map.entrySet()) {
            json.add(entry.getKey(), writeValue(entry.getValue()));
        }
        return json;
    }

    private static Map<String, Value> readMap(JsonObject json) {
        Map<String, Value> map = new HashMap<>();
        for (String key : json.keySet()) {
            map.put(key, readValue(json.get(key)));
        }
        return map;
    }

    private static JsonArrayBuilder writeList(List< Value> list) {
        JsonArrayBuilder array = createArrayBuilder();
        for (Value value : list) {
            array.add(writeValue(value));
        }
        return array;
    }

    private static List< Value> readList(JsonArray array) {
        List< Value> list = new ArrayList<>(array.size());
        for (JsonValue json : array.getValuesAs(JsonValue.class)) {
            list.add(readValue(json));
        }
        return list;
    }

    private static JsonValue writeValue(Value value) {
        JsonObjectBuilder json = createObjectBuilder();
        json.add(TYPE, value.type().name());
        switch (value.type()) {
            case NULL:
                return json.addNull(VALUE).build();

            case BYTE:
            case BYTE_ARRAY:
                return json.add(VALUE, value.asHexadecimalString()).build();

            case BOOLEAN:
                return json.add(VALUE, value.asBoolean()).build();

            case INTEGER:
                return json.add(VALUE, value.asInt()).build();

            case LONG:
                return json.add(VALUE, value.asLong()).build();

            case BIG_DECIMAL:
                return json.add(VALUE, value.asBigDecimal()).build();

            case STRING:
                return json.add(VALUE, value.asString()).build();

            case DATE:
                return json.add(VALUE, value.asDate().getTime()).build();

            case MAP:
                return json.add(VALUE, writeMap(value.asMap())).build();

            case LIST:
                return json.add(VALUE, writeList(value.asList())).build();

            default:
                throw new IllegalArgumentException(value.type().toString());
        }
    }

    private static Value readValue(JsonValue value) {
        JsonObject json = (JsonObject) value;
        ValueType type = ValueType.valueOf(json.getString(TYPE));
        switch (type) {
            case NULL:
                return Value.ofNull();

            case BYTE:
                return Value.of(base16().decode(json.getString(VALUE))[0]);

            case BYTE_ARRAY:
                return Value.of(base16().decode(json.getString(VALUE)));

            case BOOLEAN:
                return Value.of(json.getBoolean(VALUE));

            case INTEGER:
                return Value.of(json.getInt(VALUE));

            case LONG:
                return Value.of(json.getJsonNumber(VALUE).longValueExact());

            case BIG_DECIMAL:
                return Value.of(json.getJsonNumber(VALUE).bigDecimalValue());

            case STRING:
                return Value.of(json.getString(VALUE));

            case DATE:
                return Value.of(new Date(json.getJsonNumber(VALUE).longValueExact()));

            case MAP:
                return Value.of(readMap(json.getJsonObject(VALUE)));

            case LIST:
                return Value.of(readList(json.getJsonArray(VALUE)));

            default:
                throw new IllegalArgumentException(type.toString());
        }
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
     * Writes supplied list of {@link Event} to a JSON array.
     *
     * @param events A list of events.
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
     * Reads a list of {@link Event} from supplied JSON array.
     *
     * @param json A JSON array.
     * @return A list of events.
     */
    public static List<Event> readEvents(JsonArray json) {
        List<Event> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readEvent(object));
        }
        return list;
    }

    private static JsonObjectBuilder writeEvent(Event event) {
        return createObjectBuilder()
                .add(SEQ, event.getSeq())
                .add(HASH, event.getHash().encode())
                .add(TIMESTAMP, event.getTimestamp().getTime())
                .add(OPERATION, event.getOperation().name());
    }

    private static Event readEvent(JsonObject json) {
        return new EventBuilder()
                .withSeq(json.getJsonNumber(SEQ).longValue())
                .withHash(new Hash(json.getString(HASH)))
                .withTimestamp(new Date(json.getJsonNumber(TIMESTAMP).longValue()))
                .withOperation(Operation.valueOf(json.getString(OPERATION)))
                .build();
    }
}
