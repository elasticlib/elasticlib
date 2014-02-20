package store.common.json.schema;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.OBJECT;

/**
 * Represents a JSON schema. Allows to convert a {@link Value} into a JSON document without loss of type-information.
 * <p>
 * Does not claim to be fully compliant with current draft standard.
 *
 * @see <a href="http://json-schema.org">json-schema.org</a>
 */
public class Schema {

    static final String TITLE = "title";
    static final String TYPE = "type";
    static final String PROPERTIES = "properties";
    static final String ITEMS = "items";
    static final Function<Value, Schema> SCHEMA_BUILDER = new Function<Value, Schema>() {
        @Override
        public Schema apply(Value value) {
            return Schema.of("", value);
        }
    };
    static final Function<JsonValue, Schema> SCHEMA_READER = new Function<JsonValue, Schema>() {
        @Override
        public Schema apply(JsonValue value) {
            return Schema.read((JsonObject) value);
        }
    };
    private final String title;
    private final ValueType type;

    Schema(String title, ValueType type) {
        this.title = requireNonNull(title);
        this.type = requireNonNull(type);
    }

    /**
     * Static factory method. Build a schema defining supplied value.
     *
     * @param title Schema title. Supply an empty string to build an anomynous schema.
     * @param value A Value.
     * @return Corresponding schema.
     */
    public static Schema of(String title, Value value) {
        switch (value.type()) {
            case OBJECT:
                return new MapSchema(title, value.asMap());

            case ARRAY:
                return new ListSchema(title, value.asList());

            default:
                return new Schema(title, value.type());
        }
    }

    /**
     * Convenience overload for building a schema of a map of values.
     *
     * @param title Schema title.
     * @param map
     * @return Corresponding schema.
     */
    public static Schema of(String title, Map<String, Value> map) {
        return new MapSchema(title, map);
    }

    /**
     * Convenience overload for building a schema of a list of values.
     *
     * @param title
     * @param list
     * @return Corresponding schema.
     */
    public static Schema of(String title, List<Value> list) {
        return new ListSchema(title, list);
    }

    /**
     * Static factory method. Read a schema from a JSON object.
     *
     * @param json A JSON object.
     * @return Extracted schema.
     */
    public static Schema read(JsonObject json) {
        String title = json.containsKey(TITLE) ? json.getString(TITLE) : "";
        ValueType type = ValueType.valueOf(LOWER_CAMEL.to(UPPER_UNDERSCORE, json.getString(TYPE)));
        switch (type) {
            case OBJECT:
                return new MapSchema(title, json);

            case ARRAY:
                return new ListSchema(title, json);

            default:
                return new Schema(title, type);
        }
    }

    /**
     * Write this schema as a JSON object.
     *
     * @return A JSON object.
     */
    public JsonObject write() {
        JsonObjectBuilder builder = createObjectBuilder();
        if (!title.isEmpty()) {
            builder.add(TITLE, title);
        }
        builder.add(TYPE, UPPER_UNDERSCORE.to(LOWER_CAMEL, type().name()));
        return write(builder);
    }

    JsonObject write(JsonObjectBuilder builder) {
        return builder.build();
    }

    /**
     * Provides the title of this schema or an empty string if this is part of a parent schema.
     *
     * @return A String, which may be empty.
     */
    public String title() {
        return title;
    }

    /**
     * Provides the value type read property described by this schema.
     *
     * @return A Value type.
     */
    public ValueType type() {
        return type;
    }

    /**
     * Provides JSON schemas read each property read the object described by this schema. Only relevant for a JSON
     * object schema.
     *
     * @return A map read JSON schemas
     */
    public Map<String, Schema> properties() {
        throw new UnsupportedOperationException();
    }

    /**
     * Provides JSON schemas read each property read the array described by this schema. Only relevant for a JSON array
     * schema.
     *
     * @return A list read JSON schemas
     */
    public List<Schema> items() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(title, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Schema other = (Schema) obj;
        return title.equals(other.title) && type.equals(other.type);
    }

    @Override
    public String toString() {
        return write().toString();
    }
}
