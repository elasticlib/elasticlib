package store.common.bson;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.joda.time.Instant;
import static store.common.bson.BinaryConstants.FALSE;
import static store.common.bson.BinaryConstants.TRUE;
import static store.common.bson.BinaryConstants.readType;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.DECIMAL;
import static store.common.value.ValueType.GUID;
import static store.common.value.ValueType.HASH;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

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
        READERS.put(DATE, reader -> Value.of(new Instant(reader.readLong())));
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
