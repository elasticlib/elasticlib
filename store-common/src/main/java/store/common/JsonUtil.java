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
import store.common.value.Value;
import store.common.value.ValueType;

public final class JsonUtil {

    private static final String VALUE = "value";
    private static final String TYPE = "type";

    private JsonUtil() {
    }

    public static boolean hasStringValue(JsonObject json, String key) {
        if (!json.containsKey(key)) {
            return false;
        }
        return json.get(key).getValueType() == STRING;
    }

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

    public static JsonArray writeHashes(List<Hash> hashes) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (Hash hash : hashes) {
            builder.add(hash.encode());
        }
        return builder.build();
    }

    public static List<Hash> readHashes(JsonArray json) {
        List<Hash> list = new ArrayList<>();
        for (JsonString value : json.getValuesAs(JsonString.class)) {
            list.add(new Hash(value.getString()));
        }
        return list;
    }

    public static JsonArray writeContentInfos(List<ContentInfo> contentInfos) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (ContentInfo contentInfo : contentInfos) {
            builder.add(writeContentInfo(contentInfo));
        }
        return builder.build();
    }

    public static List<ContentInfo> readContentInfos(JsonArray json) {
        List<ContentInfo> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readContentInfo(object));
        }
        return list;
    }

    public static JsonObject writeContentInfo(ContentInfo contentInfo) {
        JsonObjectBuilder builder = createObjectBuilder()
                .add("hash", contentInfo.getHash().encode())
                .add("rev", contentInfo.getRev().encode());

        JsonArrayBuilder parents = createArrayBuilder();
        for (Hash item : contentInfo.getParents()) {
            parents.add(item.encode());
        }
        builder.add("parents", parents);
        if (contentInfo.isDeleted()) {
            builder.add("deleted", true);
        }
        return builder
                .add("length", contentInfo.getLength())
                .add("metadata", writeMap(contentInfo.getMetadata()))
                .build();
    }

    public static ContentInfo readContentInfo(JsonObject json) {
        ContentInfoBuilder builder = new ContentInfoBuilder()
                .withHash(new Hash(json.getString("hash")))
                .withRev(new Hash(json.getString("rev")));

        for (JsonString value : json.getJsonArray("parents").getValuesAs(JsonString.class)) {
            builder.withParent(new Hash(value.getString()));
        }
        if (json.containsKey("deleted")) {
            builder.withDeleted(json.getBoolean("deleted"));
        }
        return builder
                .withLength(json.getJsonNumber("length").longValue())
                .withMetadata(readMap(json.getJsonObject("metadata")))
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

    public static JsonObject writeConfig(Config config) {
        return createObjectBuilder()
                .add("repositories", writePaths(config.getRepositories()))
                .add("sync", writeSync(config))
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

    public static Config readConfig(JsonObject json) {
        return new Config(readPaths(json.getJsonArray("repositories")),
                          readSync(json.getJsonObject("sync")));
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

    public static JsonArray writeEvents(List<Event> events) {
        JsonArrayBuilder builder = createArrayBuilder();
        for (Event event : events) {
            builder.add(writeEvent(event));
        }
        return builder.build();
    }

    public static List<Event> readEvents(JsonArray json) {
        List<Event> list = new ArrayList<>();
        for (JsonObject object : json.getValuesAs(JsonObject.class)) {
            list.add(readEvent(object));
        }
        return list;
    }

    private static JsonObjectBuilder writeEvent(Event event) {
        return createObjectBuilder()
                .add("seq", event.getSeq())
                .add("hash", event.getHash().encode())
                .add("timestamp", event.getTimestamp().getTime())
                .add("operation", event.getOperation().name());
    }

    private static Event readEvent(JsonObject json) {
        return Event.event()
                .withSeq(json.getJsonNumber("seq").longValue())
                .withHash(new Hash(json.getString("hash")))
                .withTimestamp(new Date(json.getJsonNumber("timestamp").longValue()))
                .withOperation(Operation.valueOf(json.getString("operation")))
                .build();
    }
}
