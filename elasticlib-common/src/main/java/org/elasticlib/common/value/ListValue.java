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

import java.util.ArrayList;
import static java.util.Collections.unmodifiableList;
import java.util.List;

final class ListValue extends Value {

    private final List<Value> value;

    public ListValue(List<Value> value) {
        this.value = unmodifiableList(new ArrayList<>(value));
    }

    @Override
    public ValueType type() {
        return ValueType.ARRAY;
    }

    @Override
    public List<Value> asList() {
        return value;
    }

    @Override
    Object value() {
        return value;
    }
}
