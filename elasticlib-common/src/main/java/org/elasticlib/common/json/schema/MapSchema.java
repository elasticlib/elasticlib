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
import static com.google.common.collect.Maps.transformValues;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import static org.elasticlib.common.json.schema.Schema.PROPERTIES;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;

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
