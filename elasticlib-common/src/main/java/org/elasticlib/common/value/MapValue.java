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
package org.elasticlib.common.value;

import static java.util.Collections.unmodifiableMap;
import java.util.LinkedHashMap;
import java.util.Map;

final class MapValue extends Value {

    private final Map<String, Value> value;

    public MapValue(Map<String, Value> value) {
        this.value = unmodifiableMap(new LinkedHashMap<>(value));
    }

    @Override
    public ValueType type() {
        return ValueType.OBJECT;
    }

    @Override
    public Map<String, Value> asMap() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
