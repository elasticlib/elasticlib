package store.common.io;

import com.google.common.base.Function;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static store.common.io.BinaryConstants.FALSE;
import static store.common.io.BinaryConstants.TRUE;
import static store.common.io.BinaryConstants.decodeType;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.BIG_DECIMAL;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.BYTE;
import static store.common.value.ValueType.BYTE_ARRAY;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.LIST;
import static store.common.value.ValueType.LONG;
import static store.common.value.ValueType.MAP;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.STRING;

final class Decoding {

    private static final Map<ValueType, Function<ByteArrayReader, Value>> FUNCTIONS = new EnumMap<>(ValueType.class);

    static {
        FUNCTIONS.put(NULL, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.ofNull();
            }
        });
        FUNCTIONS.put(BYTE_ARRAY, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(reader.readByteArray(reader.readInt()));
            }
        });
        FUNCTIONS.put(BOOLEAN, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                byte b = reader.readByte();
                switch (b) {
                    case FALSE:
                        return Value.of(false);
                    case TRUE:
                        return Value.of(true);
                    default:
                        throw new IllegalArgumentException(String.format("0x%02x", b));
                }
            }
        });
        FUNCTIONS.put(BYTE, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(reader.readByte());
            }
        });
        FUNCTIONS.put(INTEGER, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(reader.readInt());
            }
        });
        FUNCTIONS.put(LONG, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(reader.readLong());
            }
        });
        FUNCTIONS.put(BIG_DECIMAL, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(new BigDecimal(reader.readString(reader.readInt())));
            }
        });
        FUNCTIONS.put(STRING, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(reader.readString(reader.readInt()));
            }
        });
        FUNCTIONS.put(DATE, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(new Date(reader.readLong()));
            }
        });
        FUNCTIONS.put(MAP, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(decodeMap(reader, reader.readInt()));
            }
        });
        FUNCTIONS.put(LIST, new Function<ByteArrayReader, Value>() {
            @Override
            public Value apply(ByteArrayReader reader) {
                return Value.of(decodeList(reader, reader.readInt()));
            }
        });
    }

    private Decoding() {
    }

    private static Value decodeValue(ByteArrayReader reader, ValueType type) {
        return FUNCTIONS.get(type).apply(reader);
    }

    public static Map<String, Value> decodeMap(ByteArrayReader reader, int length) {
        Map<String, Value> map = new LinkedHashMap<>();
        int limit = reader.position() + length;
        while (reader.position() < limit) {
            ValueType type = decodeType(reader.readByte());
            String key = reader.readNullTerminatedString();
            map.put(key, decodeValue(reader, type));
        }
        return map;
    }

    public static List<Value> decodeList(ByteArrayReader reader, int length) {
        int limit = reader.position() + length;
        List<Value> list = new ArrayList<>();
        while (reader.position() < limit) {
            ValueType type = decodeType(reader.readByte());
            list.add(decodeValue(reader, type));
        }
        return list;
    }
}
