package store.common.json;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
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

final class ValueWriting {

    private static final String VALUE = "value";
    private static final String TYPE = "type";
    private static final Map<ValueType, Procedure<JsonObjectBuilder, Value>> WRITERS = new EnumMap<>(ValueType.class);

    static {
        WRITERS.put(NULL, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.addNull(VALUE);
            }
        });
        WRITERS.put(BYTE, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asHexadecimalString());
            }
        });
        WRITERS.put(BYTE_ARRAY, WRITERS.get(BYTE));
        WRITERS.put(BOOLEAN, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asBoolean());
            }
        });
        WRITERS.put(INTEGER, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asInt());
            }
        });
        WRITERS.put(LONG, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asLong());
            }
        });
        WRITERS.put(BIG_DECIMAL, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asBigDecimal().toString());
            }
        });
        WRITERS.put(STRING, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asString());
            }
        });
        WRITERS.put(DATE, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, value.asDate().getTime());
            }
        });
        WRITERS.put(MAP, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, writeMap(value.asMap()));
            }
        });
        WRITERS.put(LIST, new Procedure<JsonObjectBuilder, Value>() {
            @Override
            public void apply(JsonObjectBuilder builder, Value value) {
                builder.add(VALUE, writeList(value.asList()));
            }
        });
    }

    private interface Procedure<U, V> {

        void apply(U u, V v);
    }

    private ValueWriting() {
    }

    public static JsonValue writeValue(Value value) {
        JsonObjectBuilder builder = createObjectBuilder();
        builder.add(TYPE, value.type().name());
        Procedure<JsonObjectBuilder, Value> proc = WRITERS.get(value.type());

        proc.apply(builder, value);
        return builder.build();
    }

    public static JsonObjectBuilder writeMap(Map<String, Value> map) {
        JsonObjectBuilder json = createObjectBuilder();
        for (Map.Entry<String, Value> entry : map.entrySet()) {
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
