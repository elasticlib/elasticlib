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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import static org.elasticlib.common.bson.BinaryConstants.FALSE;
import static org.elasticlib.common.bson.BinaryConstants.TRUE;
import static org.elasticlib.common.bson.BinaryConstants.readType;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
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

final class ValueReading {

    private static final Map<ValueType, Function<ByteArrayReader, Value>> READERS = new EnumMap<>(ValueType.class);
    private static final int HASH_LENGTH = 20;
    private static final int GUID_LENGTH = 16;

    static {
        READERS.put(NULL, reader -> Value.ofNull());
        READERS.put(HASH, reader -> Value.of(new Hash(reader.readByteArray(HASH_LENGTH))));
        READERS.put(HASH, reader -> Value.of(new Hash(reader.readByteArray(HASH_LENGTH))));
        READERS.put(GUID, reader -> Value.of(new Guid(reader.readByteArray(GUID_LENGTH))));
        READERS.put(BINARY, reader -> Value.of(reader.readByteArray(reader.readInt())));
        READERS.put(BOOLEAN, reader -> {
            byte b = reader.readByte();
            switch (b) {
                case FALSE:
                    return Value.of(false);
                case TRUE:
                    return Value.of(true);
                default:
                    throw new IllegalArgumentException(String.format("0x%02x", b));
            }
        });
        READERS.put(INTEGER, reader -> Value.of(reader.readLong()));
        READERS.put(DECIMAL, reader -> Value.of(new BigDecimal(reader.readString(reader.readInt()))));
        READERS.put(STRING, reader -> Value.of(reader.readString(reader.readInt())));
        READERS.put(DATE, reader -> Value.of(Instant.ofEpochMilli(reader.readLong())));
        READERS.put(OBJECT, reader -> Value.of(readMap(reader, reader.readInt())));
        READERS.put(ARRAY, reader -> Value.of(readList(reader, reader.readInt())));
    }

    private ValueReading() {
    }

    private static Value readValue(ByteArrayReader reader, ValueType type) {
        return READERS.get(type).apply(reader);
    }

    public static Map<String, Value> readMap(ByteArrayReader reader, int length) {
        Map<String, Value> map = new LinkedHashMap<>();
        int limit = reader.position() + length;
        while (reader.position() < limit) {
            ValueType type = readType(reader.readByte());
            String key = reader.readNullTerminatedString();
            map.put(key, readValue(reader, type));
        }
        return map;
    }

    public static List<Value> readList(ByteArrayReader reader, int length) {
        int limit = reader.position() + length;
        List<Value> list = new ArrayList<>();
        while (reader.position() < limit) {
            ValueType type = readType(reader.readByte());
            list.add(readValue(reader, type));
        }
        return list;
    }
}
