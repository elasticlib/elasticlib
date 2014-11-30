package store.common.json;

import static com.google.common.io.BaseEncoding.base64;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.joda.time.Instant;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.json.schema.Schema;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.DECIMAL;
import static store.common.value.ValueType.GUID;
import static store.common.value.ValueType.HASH;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

final class ValueReading {

    private static final Map<ValueType, Reader> READERS = new EnumMap<>(ValueType.class);

    static {
        READERS.put(NULL, (json, schema) -> Value.ofNull());
        READERS.put(HASH, (json, schema) -> Value.of(new Hash(asString(json))));
        READERS.put(GUID, (json, schema) -> Value.of(new Guid(asString(json))));
        READERS.put(BINARY, (json, schema) -> Value.of(base64().decode(asString(json))));
        READERS.put(BOOLEAN, (json, schema) -> Value.of(json.getValueType() == JsonValue.ValueType.TRUE));
        READERS.put(INTEGER, (json, schema) -> Value.of(jsonNumber(json).longValueExact()));
        READERS.put(DECIMAL, (json, schema) -> Value.of(jsonNumber(json).bigDecimalValue()));
        READERS.put(STRING, (json, schema) -> Value.of(asString(json)));
        READERS.put(DATE, (json, schema) -> Value.of(new Instant(jsonNumber(json).longValueExact())));
        READERS.put(OBJECT, (json, schema) -> Value.of(readMap((JsonObject) json, schema)));
        READERS.put(ARRAY, (json, schema) -> Value.of(readList((JsonArray) json, schema)));
    }

    @FunctionalInterface
    private interface Reader {

        Value apply(JsonValue value, Schema schema);
    }

    private static String asString(JsonValue json) {
        return JsonString.class.cast(json).getString();
    }

    private static JsonNumber jsonNumber(JsonValue json) {
        return JsonNumber.class.cast(json);
    }

    private ValueReading() {
    }

    public static Value readValue(JsonValue value, Schema schema) {
        return READERS.get(schema.type()).apply(value, schema);
    }

    public static Map<String, Value> readMap(JsonObject json, Schema schema) {
        Map<String, Value> map = new LinkedHashMap<>();
        json.keySet().stream()
                .filter(key -> schema.properties().containsKey(key))
                .forEach(key -> {
                    Schema subSchema = schema.properties().get(key);
                    if (!subSchema.definition().isEmpty()) {
                        subSchema = Schema.read(json.getJsonObject(subSchema.definition()));
                    }
                    map.put(key, readValue(json.get(key), subSchema));
                });
        return map;
    }

    public static List<Value> readList(JsonArray array, Schema schema) {
        List<Value> list = new ArrayList<>(array.size());
        List<Schema> itemsSchemas = schema.items();
        if (itemsSchemas.size() == 1) {
            array.getValuesAs(JsonValue.class)
                    .stream()
                    .forEach(json -> list.add(readValue(json, itemsSchemas.get(0))));
        } else {
            for (int i = 0; i < array.size(); i++) {
                list.add(readValue(array.get(i), itemsSchemas.get(i)));
            }
        }
        return list;
    }
}
