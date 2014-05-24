package store.common.json;

import java.util.List;
import java.util.Map.Entry;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import static javax.json.JsonValue.ValueType.FALSE;
import static javax.json.JsonValue.ValueType.STRING;
import static javax.json.JsonValue.ValueType.TRUE;
import store.common.json.schema.Schema;
import store.common.value.ValueType;
import static store.common.value.ValueType.DECIMAL;
import static store.common.value.ValueType.HASH;

/**
 * JSON validation utils.
 */
public final class JsonValidation {

    private JsonValidation() {
    }

    /**
     * Checks if supplied JSON object has a string value associated to supplied key.
     *
     * @param json A JSON object.
     * @param key A key.
     * @return <tt>true</tt> if supplied JSON object has a string getCode associated to supplied key.
     */
    public static boolean hasStringValue(JsonObject json, String key) {
        return hasValue(json, key, STRING);
    }

    /**
     * Checks if supplied JSON object has a boolean value associated to supplied key.
     *
     * @param json A JSON object.
     * @param key A key.
     * @return <tt>true</tt> if supplied JSON object has a boolean getCode associated to supplied key.
     */
    public static boolean hasBooleanValue(JsonObject json, String key) {
        return hasValue(json, key, TRUE, FALSE);
    }

    private static boolean hasValue(JsonObject json, String key, JsonValue.ValueType... types) {
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
     * Checks if supplied JSON object is a valid representation of the supplied class.
     *
     * @param json A JSON object.
     * @param clazz The class to check JSON against.
     * @return <tt>true</tt> if an instance of supplied class can be read from supplied JSON object.
     */
    public static boolean isValid(JsonObject json, Class<?> clazz) {
        return isValid(json, SchemaProvider.getSchema(clazz));
    }

    // Package-private for testing.
    static boolean isValid(JsonObject json, Schema schema) {
        if (schema.type() != ValueType.OBJECT) {
            return false;
        }
        for (Entry<String, Schema> property : schema.properties().entrySet()) {
            String key = property.getKey();
            Schema propertySchema = property.getValue();
            if (!isPropertyValid(json, key, propertySchema)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPropertyValid(JsonObject json, String key, Schema propertySchema) {
        if (!json.containsKey(key)) {
            return propertySchema.isOptional();
        }
        if (!propertySchema.definition().isEmpty()) {
            if (!json.containsKey(propertySchema.definition())) {
                return false;
            }
            propertySchema = Schema.read(json.getJsonObject(propertySchema.definition()));
        }
        return isValid(json.get(key), propertySchema);
    }

    private static boolean isValid(JsonArray array, Schema schema) {
        if (schema.type() != ValueType.ARRAY) {
            return false;
        }
        if (schema.items().size() == 1) {
            return areItemsValid(array, schema.items().get(0));
        }
        return areItemsValid(array, schema.items());
    }

    private static boolean areItemsValid(JsonArray array, Schema itemSchema) {
        for (JsonValue value : array) {
            if (!isValid(value, itemSchema)) {
                return false;
            }
        }
        return true;
    }

    private static boolean areItemsValid(JsonArray array, List<Schema> itemSchemas) {
        if (array.size() != itemSchemas.size()) {
            return false;
        }
        for (int i = 0; i < array.size(); i++) {
            if (!isValid(array.get(i), itemSchemas.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValid(JsonValue value, Schema schema) {
        switch (value.getValueType()) {
            case OBJECT:
                return isValid((JsonObject) value, schema);

            case ARRAY:
                return isValid((JsonArray) value, schema);

            case NULL:
                return schema.type() == ValueType.NULL;

            case TRUE:
            case FALSE:
                return schema.type() == ValueType.BOOLEAN;

            case STRING:
                return isValid(((JsonString) value).getString(), schema.type());

            case NUMBER:
                return isValid((JsonNumber) value, schema.type());

            default:
                throw new AssertionError();
        }
    }

    private static boolean isValid(String value, ValueType type) {
        switch (type) {
            case HASH:
                return value.length() == 40 && value.matches("[0-9a-fA-F]*");

            case BINARY:
                return value.matches("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$");

            case STRING:
                return true;

            default:
                return false;

        }
    }

    private static boolean isValid(JsonNumber value, ValueType type) {
        switch (type) {
            case INTEGER:
            case DATE:
                return isLong(value);

            case DECIMAL:
                // JsonNumber.bigDecimalValue() is not expected to fail.
                return true;

            default:
                return false;
        }
    }

    private static boolean isLong(JsonNumber value) {
        try {
            value.longValueExact();
            return true;

        } catch (ArithmeticException e) {
            return false;
        }
    }
}
