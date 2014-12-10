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
package org.elasticlib.common.bson;

import java.nio.ByteBuffer;
import java.util.Arrays;

class ByteArrayBuilder {

    private byte[] array;
    private int size;

    public ByteArrayBuilder() {
        this(32);
    }

    public ByteArrayBuilder(int capacity) {
        array = new byte[capacity];
        this.size = 0;
    }

    public ByteArrayBuilder append(byte b) {
        ensureCapacity(size + 1);
        array[size] = b;
        size++;
        return this;
    }

    public ByteArrayBuilder append(byte[] bytes) {
        ensureCapacity(size + bytes.length);
        System.arraycopy(bytes, 0, array, size, bytes.length);
        size += bytes.length;
        return this;
    }

    private void ensureCapacity(int capacity) {
        if (array.length < capacity) {
            int length = array.length;
            while (length < capacity) {
                length *= 2;
            }
            array = Arrays.copyOf(array, length);
        }
    }

    public byte[] build() {
        return Arrays.copyOf(array, size);
    }

    public byte[] prependSizeAndBuild() {
        return new ByteArrayBuilder(size + 4)
                .append(encodeSize())
                .append(build())
                .build();
    }

    private byte[] encodeSize() {
        return ByteBuffer.allocate(4)
                .putInt(size)
                .array();
    }
}
