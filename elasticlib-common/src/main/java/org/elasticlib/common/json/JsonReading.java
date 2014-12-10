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
package org.elasticlib.common.json;

import java.util.List;
import java.util.Map;
import static java.util.stream.Collectors.toList;
import javax.json.JsonArray;
import javax.json.JsonObject;
import org.elasticlib.common.json.schema.Schema;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.mappable.MappableUtil;
import org.elasticlib.common.value.Value;

/**
 * JSON reading utils.
 */
public final class JsonReading {

    private JsonReading() {
    }

    /**
     * Reads a {@link Mappable} from supplied JSON object.
     *
     * @param <T> Actual class to read.
     * @param json A JSON object.
     * @param clazz Actual class to read.
     * @return A new instance of supplied class.
     */
    public static <T extends Mappable> T read(JsonObject json, Class<T> clazz) {
        Schema schema = SchemaProvider.getSchema(clazz);
        Map<String, Value> values = ValueReading.readMap(json, schema);
        return MappableUtil.fromMap(values, clazz);
    }

    /**
     * Reads a list of {@link Mappable} from supplied JSON array.
     *
     * @param <T> Actual class to read.
     * @param array A JSON array.
     * @param clazz Actual class to read.
     * @return A list of new instances of supplied class.
     */
    public static <T extends Mappable> List<T> readAll(JsonArray array, Class<T> clazz) {
        return array.getValuesAs(JsonObject.class)
                .stream()
                .map(json -> read(json, clazz))
                .collect(toList());
    }
}
