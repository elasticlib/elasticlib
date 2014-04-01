package store.common.json;

import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import store.common.json.schema.Schema;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BIG_DECIMAL;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

final class ValueWriting {

    private static final Map<ValueType, Writer> WRITERS = new EnumMap<>(ValueType.class);

    static {
        WRITERS.put(NULL, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return JsonValue.NULL;
            }
        });
        WRITERS.put(BINARY, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return jsonString(value.asHexadecimalString());
            }
        });
        WRITERS.put(BOOLEAN, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return value.asBoolean() ? JsonValue.TRUE : JsonValue.FALSE;
            }
        });
        WRITERS.put(LONG, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return jsonNumber(value.asLong());
            }
        });
        WRITERS.put(BIG_DECIMAL, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return jsonNumber(value.asBigDecimal());
            }
        });
        WRITERS.put(STRING, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return jsonString(value.asString());
            }
        });
        WRITERS.put(DATE, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return jsonNumber(value.asDate().getTime());
            }
        });
        WRITERS.put(OBJECT, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return writeMap(value.asMap(), schema).build();
            }
        });
        WRITERS.put(ARRAY, new Writer() {
            @Override
            public JsonValue apply(Value value, Schema schema) {
                return writeList(value.asList(), schema).build();
            }
        });
    }

    private interface Writer {

        JsonValue apply(Value value, Schema schema);
    }

    private ValueWriting() {
    }

    private static JsonString jsonString(String value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonString(0);
    }

    private static JsonNumber jsonNumber(long value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonNumber(0);
    }

    private static JsonNumber jsonNumber(BigDecimal value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonNumber(0);
    }

    public static JsonValue writeValue(Value value, Schema schema) {
        return WRITERS.get(value.type()).apply(value, schema);
    }

    public static JsonObjectBuilder writeMap(Map<String, Value> map, Schema schema) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Entry<String, Value> entry : map.entrySet()) {
            String key = entry.getKey();
            Value value = entry.getValue();
            Schema subSchema = schema.properties().get(key);
            if (!subSchema.definition().isEmpty()) {
                Schema definition = Schema.of(key, value);
                json.add(subSchema.definition(), definition.write());
                json.add(key, writeValue(value, definition));
            } else {
                json.add(key, writeValue(value, subSchema));
            }
        }
        return json;
    }

    public static JsonArrayBuilder writeList(List<Value> list, Schema schema) {
        JsonArrayBuilder array = createArrayBuilder();
        List<Schema> itemsSchemas = schema.items();
        if (itemsSchemas.size() == 1) {
            for (Value value : list) {
                array.add(writeValue(value, itemsSchemas.get(0)));
            }
        } else {
            for (int i = 0; i < list.size(); i++) {
                array.add(writeValue(list.get(i), itemsSchemas.get(i)));
            }
        }
        return array;
    }
}
