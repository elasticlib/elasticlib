package store.common.json.schema;

import static com.google.common.collect.Lists.transform;
import java.util.Collections;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import static javax.json.Json.createArrayBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import static store.common.json.schema.Schema.ITEMS;
import static store.common.json.schema.Schema.PROPERTIES;
import static store.common.json.schema.Schema.SCHEMA_BUILDER;
import static store.common.json.schema.Schema.SCHEMA_READER;
import store.common.value.Value;
import store.common.value.ValueType;

final class ListSchema extends Schema {

    private final List<Schema> items;

    ListSchema(String title, List<Value> list) {
        super(title, ValueType.LIST);
        items = unmodifiableList(transform(list, SCHEMA_BUILDER));
    }

    ListSchema(String title, JsonObject json) {
        super(title, ValueType.LIST);
        if (json.get(ITEMS).getValueType() == JsonValue.ValueType.OBJECT) {
            items = Collections.singletonList(Schema.read(json.getJsonObject(ITEMS)));
        } else {
            items = unmodifiableList(transform(json.getJsonArray(ITEMS), SCHEMA_READER));
        }
    }

    @Override
    JsonObject write(JsonObjectBuilder builder) {
        if (items.size() == 1) {
            return builder
                    .add(ITEMS, items.get(0).write())
                    .build();
        }
        JsonArrayBuilder itemsBuilder = createArrayBuilder();
        for (Schema item : items) {
            itemsBuilder.add(item.write());
        }
        return builder
                .add(PROPERTIES, itemsBuilder)
                .build();
    }

    @Override
    public ValueType type() {
        return ValueType.LIST;
    }

    @Override
    public List<Schema> items() {
        return items;
    }
}
