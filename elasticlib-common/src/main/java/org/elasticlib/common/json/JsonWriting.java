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
import static javax.json.Json.createArrayBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import org.elasticlib.common.json.schema.Schema;
import org.elasticlib.common.mappable.Mappable;

/**
 * JSON writing utils.
 */
public final class JsonWriting {

    private JsonWriting() {
    }

    /**
     * Writes supplied {@link Mappable} to a JSON object.
     *
     * @param mappable A mappable instance.
     * @return A JSON object.
     */
    public static JsonObject write(Mappable mappable) {
        Schema schema = SchemaProvider.getSchema(mappable.getClass());
        return ValueWriting.writeMap(mappable.toMap(), schema).build();
    }

    /**
     * Writes supplied list of {@link Mappable} to a JSON array.
     *
     * @param mappables A list of mappables.
     * @return A JSON array.
     */
    public static JsonArray writeAll(List<? extends Mappable> mappables) {
        JsonArrayBuilder array = createArrayBuilder();
        mappables.forEach(mappable -> array.add(write(mappable)));
        return array.build();
    }
}
