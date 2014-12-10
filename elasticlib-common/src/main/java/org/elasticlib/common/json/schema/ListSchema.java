/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.json.schema;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import static com.google.common.collect.Lists.transform;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import static javax.json.Json.createArrayBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import static org.elasticlib.common.json.schema.Schema.ITEMS;
import static org.elasticlib.common.json.schema.Schema.SCHEMA_BUILDER;
import static org.elasticlib.common.json.schema.Schema.SCHEMA_READER;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;

final class ListSchema extends Schema {

    private final List<Schema> items;

    ListSchema(String title, List<Value> list) {
        super(title, ValueType.ARRAY, false);
        final List<Schema> tmp = transform(list, SCHEMA_BUILDER);
        if (!tmp.isEmpty() && Iterables.all(tmp, schema -> schema.equals(tmp.get(0)))) {
            items = singletonList(tmp.get(0));

        } else {
            items = unmodifiableList(tmp);
        }
    }

    ListSchema(String title, JsonObject json, boolean optional) {
        super(title, ValueType.ARRAY, optional);
        if (json.get(ITEMS).getValueType() == JsonValue.ValueType.OBJECT) {
            items = singletonList(Schema.read(json.getJsonObject(ITEMS)));
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
        items.forEach(item -> itemsBuilder.add(item.write()));
        return builder
                .add(ITEMS, itemsBuilder)
                .build();
    }

    @Override
    public ValueType type() {
        return ValueType.ARRAY;
    }

    @Override
    public List<Schema> items() {
        return items;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), items);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        ListSchema other = (ListSchema) obj;
        return items.equals(other.items);
    }
}
