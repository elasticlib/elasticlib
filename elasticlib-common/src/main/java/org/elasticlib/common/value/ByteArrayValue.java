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

import static com.google.common.io.BaseEncoding.base16;
import java.util.Arrays;

final class ByteArrayValue extends Value {

    private final byte[] value;

    /**
     * Constructor.
     *
     * @param value Actual wrapped value.
     */
    public ByteArrayValue(byte[] value) {
        this.value = Arrays.copyOf(value, value.length);
    }

    @Override
    public ValueType type() {
        return ValueType.BINARY;
    }

    @Override
    public byte[] asByteArray() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public String toString() {
        return base16()
                .lowerCase()
                .encode(value);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + type().hashCode();
        hash = 19 * hash + Arrays.hashCode(value);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ByteArrayValue)) {
            return false;
        }
        ByteArrayValue other = (ByteArrayValue) obj;
        return Arrays.equals(value, other.value);
    }

    @Override
    Object value() {
        throw new UnsupportedOperationException();
    }
}
