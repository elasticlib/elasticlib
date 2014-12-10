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
import com.google.common.base.Function;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import static org.elasticlib.common.bson.BinaryConstants.FALSE;
import static org.elasticlib.common.bson.BinaryConstants.NULL_BYTE;
import static org.elasticlib.common.bson.BinaryConstants.TRUE;
import static org.elasticlib.common.bson.BinaryConstants.writeType;
import org.elasticlib.common.value.Value;
import org.elasticlib.common.value.ValueType;
import static org.elasticlib.common.value.ValueType.ARRAY;
import static org.elasticlib.common.value.ValueType.BINARY;
import static org.elasticlib.common.value.ValueType.BOOLEAN;
import static org.elasticlib.common.value.ValueType.DATE;
import static org.elasticlib.common.value.ValueType.DECIMAL;
import static org.elasticlib.common.value.ValueType.GUID;
import static org.elasticlib.common.value.ValueType.HASH;
import static org.elasticlib.common.value.ValueType.INTEGER;
import static org.elasticlib.common.value.ValueType.NULL;
import static org.elasticlib.common.value.ValueType.OBJECT;
import static org.elasticlib.common.value.ValueType.STRING;

final class ValueWriting {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Map<ValueType, Function<Value, byte[]>> WRITERS = new EnumMap<>(ValueType.class);

    static {
        WRITERS.put(NULL, value -> EMPTY_BYTE_ARRAY);
        WRITERS.put(HASH, value -> value.asHash().getBytes());
        WRITERS.put(GUID, value -> value.asGuid().getBytes());
        WRITERS.put(BINARY, value -> writeByteArray(value.asByteArray()));
        WRITERS.put(BOOLEAN, value -> new byte[]{value.asBoolean() ? TRUE : FALSE});
        WRITERS.put(INTEGER, value -> writeLong(value.asLong()));
        WRITERS.put(DECIMAL, value -> writeString(value.toString()));
        WRITERS.put(STRING, value -> writeString(value.asString()));
        WRITERS.put(DATE, value -> writeLong(value.asInstant().toEpochMilli()));
        WRITERS.put(OBJECT, value -> {
            ByteArrayBuilder builder = new ByteArrayBuilder();
            value.asMap().entrySet().forEach(entry -> {
                builder.append(writeType(entry.getValue().type()))
                        .append(writeKey(entry.getKey()))
                        .append(writeValue(entry.getValue()));
            });
            return builder.prependSizeAndBuild();
        });
        WRITERS.put(ARRAY, value -> {
            ByteArrayBuilder builder = new ByteArrayBuilder();
            value.asList().forEach(item -> {
                builder.append(writeType(item.type()))
                        .append(writeValue(item));
            });
            return builder.prependSizeAndBuild();
        });
    }

    private ValueWriting() {
    }

    private static byte[] writeByteArray(byte[] value) {
        return new ByteArrayBuilder(value.length + 4)
                .append(writeInt(value.length))
                .append(value)
                .build();
    }

    private static byte[] writeInt(int value) {
        return ByteBuffer.allocate(4)
                .putInt(value)
                .array();
    }

    private static byte[] writeLong(long value) {
        return ByteBuffer.allocate(8)
                .putLong(value)
                .array();
    }

    private static byte[] writeString(String value) {
        return writeByteArray(value.getBytes(UTF_8));
    }

    public static byte[] writeKey(String key) {
        byte[] bytes = key.getBytes(UTF_8);
        return new ByteArrayBuilder(bytes.length + 1)
                .append(bytes)
                .append(NULL_BYTE)
                .build();
    }

    public static byte[] writeValue(Value value) {
        return WRITERS.get(value.type()).apply(value);
    }
}
