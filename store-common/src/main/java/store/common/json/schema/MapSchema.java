package store.common.json.schema;

import com.google.common.base.Objects;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import static store.common.json.schema.Schema.PROPERTIES;
import store.common.value.Value;
import store.common.value.ValueType;

final class MapSchema extends Schema {

    private final Map<String, Schema> properties;

    MapSchema(String title, Map<String, Value> map) {
        super(title, ValueType.OBJECT, false);
        properties = unmodifiableMap(transformValues(map, SCHEMA_BUILDER));
    }

    MapSchema(String title, JsonObject jsonObject, boolean optional) {
        super(title, ValueType.OBJECT, optional);
        properties = unmodifiableMap(transformValues(jsonObject.getJsonObject(PROPERTIES), SCHEMA_READER));
    }

    @Override
    JsonObject write(JsonObjectBuilder builder) {
        JsonObjectBuilder propertiesBuilder = createObjectBuilder();
        properties.entrySet()
                .stream()
                .forEach(entry -> propertiesBuilder.add(entry.getKey(), entry.getValue().write()));

        return builder
                .add(PROPERTIES, propertiesBuilder)
                .build();
    }

    @Override
    public Map<String, Schema> properties() {
        return properties;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), properties);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        MapSchema other = (MapSchema) obj;
        return properties.equals(other.properties);
    }
}
