package store.common.json;

import com.google.common.base.Function;
import static com.google.common.io.BaseEncoding.base16;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.BIG_DECIMAL;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.BYTE;
import static store.common.value.ValueType.BYTE_ARRAY;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.LIST;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.MAP;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.STRING;

final class ValueReading {

    private static final String VALUE = "value";
    private static final String TYPE = "type";
    private static final Map<ValueType, Function<JsonObject, Value>> READERS = new EnumMap<>(ValueType.class);

    static {
        READERS.put(NULL, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.ofNull();
            }
        });
        READERS.put(BYTE, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(base16().decode(json.getString(VALUE).toUpperCase())[0]);
            }
        });
        READERS.put(BYTE_ARRAY, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(base16().decode(json.getString(VALUE).toUpperCase()));
            }
        });
        READERS.put(BOOLEAN, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(json.getBoolean(VALUE));
            }
        });
        READERS.put(INTEGER, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(json.getInt(VALUE));
            }
        });
        READERS.put(LONG, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(json.getJsonNumber(VALUE).longValueExact());
            }
        });
        READERS.put(BIG_DECIMAL, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(new BigDecimal(json.getString(VALUE)));
            }
        });
        READERS.put(STRING, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(json.getString(VALUE));
            }
        });
        READERS.put(DATE, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(new Date(json.getJsonNumber(VALUE).longValueExact()));
            }
        });
        READERS.put(MAP, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(readMap(json.getJsonObject(VALUE)));
            }
        });
        READERS.put(LIST, new Function<JsonObject, Value>() {
            @Override
            public Value apply(JsonObject json) {
                return Value.of(readList(json.getJsonArray(VALUE)));
            }
        });
    }

    private ValueReading() {
    }

    public static Value readValue(JsonValue value) {
        JsonObject json = (JsonObject) value;
        ValueType type = ValueType.valueOf(json.getString(TYPE));
        return READERS.get(type).apply(json);
    }

    public static Map<String, Value> readMap(JsonObject json) {
        Map<String, Value> map = new HashMap<>();
        for (String key : json.keySet()) {
            map.put(key, readValue(json.get(key)));
        }
        return map;
    }

    public static List<Value> readList(JsonArray array) {
        List<Value> list = new ArrayList<>(array.size());
        for (JsonValue json : array.getValuesAs(JsonValue.class)) {
            list.add(readValue(json));
        }
        return list;
    }
}
