package store.common.json;

import static com.google.common.io.BaseEncoding.base16;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import store.common.json.schema.Schema;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BIG_DECIMAL;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.BYTE;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.INT;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

final class ValueReading {

    private static final Map<ValueType, Reader> READERS = new EnumMap<>(ValueType.class);

    static {
        READERS.put(NULL, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.ofNull();
            }
        });
        READERS.put(BYTE, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(base16().decode(asString(json).toUpperCase())[0]);
            }
        });
        READERS.put(BINARY, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(base16().decode(asString(json).toUpperCase()));
            }
        });
        READERS.put(BOOLEAN, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(json.getValueType() == JsonValue.ValueType.TRUE);
            }
        });
        READERS.put(INT, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(jsonNumber(json).intValueExact());
            }
        });
        READERS.put(LONG, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(jsonNumber(json).longValueExact());
            }
        });
        READERS.put(BIG_DECIMAL, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(jsonNumber(json).bigDecimalValue());
            }
        });
        READERS.put(STRING, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(asString(json));
            }
        });
        READERS.put(DATE, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(new Date(jsonNumber(json).longValueExact()));
            }
        });
        READERS.put(OBJECT, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(readMap((JsonObject) json, schema));
            }
        });
        READERS.put(ARRAY, new Reader() {
            @Override
            public Value apply(JsonValue json, Schema schema) {
                return Value.of(readList((JsonArray) json, schema));
            }
        });
    }

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
        for (String key : json.keySet()) {
            if (schema.properties().containsKey(key)) {
                Schema subSchema = schema.properties().get(key);
                if (!subSchema.definition().isEmpty()) {
                    subSchema = Schema.read(json.getJsonObject(subSchema.definition()));
                }
                map.put(key, readValue(json.get(key), subSchema));
            }
        }
        return map;
    }

    public static List<Value> readList(JsonArray array, Schema schema) {
        List<Value> list = new ArrayList<>(array.size());
        List<Schema> itemsSchemas = schema.items();
        if (itemsSchemas.size() == 1) {
            for (JsonValue json : array.getValuesAs(JsonValue.class)) {
                list.add(readValue(json, itemsSchemas.get(0)));
            }
        } else {
            for (int i = 0; i < array.size(); i++) {
                list.add(readValue(array.get(i), itemsSchemas.get(i)));
            }
        }
        return list;
    }
}
