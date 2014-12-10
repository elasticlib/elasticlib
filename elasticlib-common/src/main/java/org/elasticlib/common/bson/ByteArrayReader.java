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

import static com.google.common.base.Charsets.UTF_8;
import java.nio.ByteBuffer;
import java.util.Arrays;
import static org.elasticlib.common.bson.BinaryConstants.NULL_BYTE;

class ByteArrayReader {

    private final byte[] bytes;
    private int position;

    public ByteArrayReader(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        position = 0;
    }

    public byte[] readByteArray(int length) {
        byte[] value = Arrays.copyOfRange(bytes, position, position + length);
        position += length;
        return value;
    }

    public byte readByte() {
        byte b = bytes[position];
        position++;
        return b;
    }

    public int readInt() {
        int value = ByteBuffer.wrap(bytes, position, 4)
                .getInt();
        position += 4;
        return value;
    }

    public long readLong() {
        long value = ByteBuffer.wrap(bytes, position, 8)
                .getLong();
        position += 8;
        return value;
    }

    public String readString(int length) {
        String str = new String(bytes, position, length, UTF_8);
        position += length;
        return str;
    }

    public String readNullTerminatedString() {
        for (int i = position; i < bytes.length; i++) {
            if (bytes[i] == NULL_BYTE) {
                String str = new String(bytes, position, i - position, UTF_8);
                position = i + 1;
                return str;
            }
        }
        throw new IllegalStateException();
    }

    public int position() {
        return position;
    }

    public int length() {
        return bytes.length;
    }
}
