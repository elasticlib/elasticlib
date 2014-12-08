package org.elasticlib.common.json;

import static com.google.common.io.BaseEncoding.base64;
import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.elasticlib.common.json.schema.Schema;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import static org.elasticlib.common.value.ValueType.ARRAY;
import static org.elasticlib.common.value.ValueType.BINARY;
import static org.elasticlib.common.value.ValueType.BOOLEAN;
import static org.elasticlib.common.value.ValueType.DATE;
import static org.elasticlib.common.value.ValueType.DECIMAL;
import static org.elasticlib.common.value.ValueType.GUID;
import static org.elasticlib.common.value.ValueType.HASH;
import static org.elasticlib.common.value.ValueType.INTEGER;
import static org.elasticlib.common.value.ValueType.NULL;
import static org.elasticlib.common.value.ValueType.OBJECT;
import static org.elasticlib.common.value.ValueType.STRING;

final class ValueWriting {

    private static final Map<ValueType, Writer> WRITERS = new EnumMap<>(ValueType.class);

    static {
        WRITERS.put(NULL, (value, schema) -> JsonValue.NULL);
        WRITERS.put(HASH, (value, schema) -> jsonString(value.asHash().asHexadecimalString()));
        WRITERS.put(GUID, (value, schema) -> jsonString(value.asGuid().asHexadecimalString()));
        WRITERS.put(BINARY, (value, schema) -> jsonString(base64().encode(value.asByteArray())));
        WRITERS.put(BOOLEAN, (value, schema) -> value.asBoolean() ? JsonValue.TRUE : JsonValue.FALSE);
        WRITERS.put(INTEGER, (value, schema) -> jsonNumber(value.asLong()));
        WRITERS.put(DECIMAL, (value, schema) -> jsonNumber(value.asBigDecimal()));
        WRITERS.put(STRING, (value, schema) -> jsonString(value.asString()));
        WRITERS.put(DATE, (value, schema) -> jsonNumber(value.asInstant().toEpochMilli()));
        WRITERS.put(OBJECT, (value, schema) -> writeMap(value.asMap(), schema).build());
        WRITERS.put(ARRAY, (value, schema) -> writeList(value.asList(), schema).build());
    }

    @FunctionalInterface
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
        map.entrySet().forEach(entry -> {
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
        });
        return json;
    }

    public static JsonArrayBuilder writeList(List<Value> list, Schema schema) {
        JsonArrayBuilder array = createArrayBuilder();
        List<Schema> itemsSchemas = schema.items();
        if (itemsSchemas.size() == 1) {
            list.forEach(value -> array.add(writeValue(value, itemsSchemas.get(0))));

        } else {
            for (int i = 0; i < list.size(); i++) {
                array.add(writeValue(list.get(i), itemsSchemas.get(i)));
            }
        }
        return array;
    }
}
