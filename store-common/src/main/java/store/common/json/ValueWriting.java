package store.common.json;

import com.google.common.base.Function;
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
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.BIG_DECIMAL;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.BYTE;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.INT;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.STRING;

final class ValueWriting {

    private static final Map<ValueType, Function<Value, JsonValue>> WRITERS = new EnumMap<>(ValueType.class);

    static {
        WRITERS.put(NULL, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return JsonValue.NULL;
            }
        });
        WRITERS.put(BYTE, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return jsonString(value.asHexadecimalString());
            }
        });
        WRITERS.put(BINARY, WRITERS.get(BYTE));
        WRITERS.put(BOOLEAN, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return value.asBoolean() ? JsonValue.TRUE : JsonValue.FALSE;
            }
        });
        WRITERS.put(INT, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return jsonNumber(value.asInt());
            }
        });
        WRITERS.put(LONG, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return jsonNumber(value.asLong());
            }
        });
        WRITERS.put(BIG_DECIMAL, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return jsonNumber(value.asBigDecimal());
            }
        });
        WRITERS.put(STRING, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return jsonString(value.asString());
            }
        });
        WRITERS.put(DATE, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return jsonNumber(value.asDate().getTime());
            }
        });
        WRITERS.put(OBJECT, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return writeMap(value.asMap()).build();
            }
        });
        WRITERS.put(ARRAY, new Function<Value, JsonValue>() {
            @Override
            public JsonValue apply(Value value) {
                return writeList(value.asList()).build();
            }
        });
    }

    private ValueWriting() {
    }

    private static JsonString jsonString(String value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonString(0);
    }

    private static JsonNumber jsonNumber(int value) {
        JsonArrayBuilder builder = createArrayBuilder();
        builder.add(value);
        return builder.build().getJsonNumber(0);
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

    public static JsonValue writeValue(Value value) {
        return WRITERS.get(value.type()).apply(value);
    }

    public static JsonObjectBuilder writeMap(Map<String, Value> map) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Entry<String, Value> entry : map.entrySet()) {
            json.add(entry.getKey(), writeValue(entry.getValue()));
        }
        return json;
    }

    public static JsonArrayBuilder writeList(List<Value> list) {
        JsonArrayBuilder array = createArrayBuilder();
        for (Value value : list) {
            array.add(writeValue(value));
        }
        return array;
    }
}
