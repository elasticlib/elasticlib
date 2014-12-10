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

import static java.util.Objects.hash;

final class NullValue extends Value {

    private static final NullValue INSTANCE = new NullValue();

    private NullValue() {
    }

    public static NullValue getInstance() {
        return INSTANCE;
    }

    @Override
    public ValueType type() {
        return ValueType.NULL;
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public int hashCode() {
        return hash(type());
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    Object value() {
        throw new UnsupportedOperationException();
    }
}
