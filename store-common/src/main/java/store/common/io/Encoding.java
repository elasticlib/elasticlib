package store.common.io;

import static com.google.common.base.Charsets.UTF_8;
import com.google.common.base.Function;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import static store.common.io.BinaryConstants.*;
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

final class Encoding {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Map<ValueType, Function<Value, byte[]>> FUNCTIONS = new EnumMap<>(ValueType.class);

    static {
        FUNCTIONS.put(NULL, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return EMPTY_BYTE_ARRAY;
            }
        });
        FUNCTIONS.put(BYTE_ARRAY, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return encodeByteArray(value.asByteArray());
            }
        });
        FUNCTIONS.put(BOOLEAN, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return new byte[]{value.asBoolean() ? TRUE : FALSE};
            }
        });
        FUNCTIONS.put(BYTE, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return new byte[]{value.asByte()};
            }
        });
        FUNCTIONS.put(INTEGER, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return encodeInt(value.asInt());
            }
        });
        FUNCTIONS.put(LONG, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return encodeLong(value.asLong());
            }
        });
        FUNCTIONS.put(BIG_DECIMAL, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return encodeString(value.toString());
            }
        });
        FUNCTIONS.put(STRING, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return encodeString(value.asString());
            }
        });
        FUNCTIONS.put(DATE, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return encodeLong(value.asDate().getTime());
            }
        });
        FUNCTIONS.put(MAP, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                ByteArrayBuilder builder = new ByteArrayBuilder();
                for (Map.Entry<String, Value> entry : value.asMap().entrySet()) {
                    builder.append(encodeType(entry.getValue().type()))
                            .append(encodeKey(entry.getKey()))
                            .append(encodeValue(entry.getValue()));
                }
                return builder.prependSizeAndBuild();
            }
        });
        FUNCTIONS.put(LIST, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                ByteArrayBuilder builder = new ByteArrayBuilder();
                for (Value item : value.asList()) {
                    builder.append(encodeType(item.type()))
                            .append(encodeValue(item));
                }
                return builder.prependSizeAndBuild();
            }
        });
    }

    private Encoding() {
    }

    private static byte[] encodeByteArray(byte[] value) {
        return new ByteArrayBuilder(value.length + 4)
                .append(encodeInt(value.length))
                .append(value)
                .build();
    }

    private static byte[] encodeInt(int value) {
        return ByteBuffer.allocate(4)
                .putInt(value)
                .array();
    }

    private static byte[] encodeLong(long value) {
        return ByteBuffer.allocate(8)
                .putLong(value)
                .array();
    }

    private static byte[] encodeString(String value) {
        return encodeByteArray(value.getBytes(UTF_8));
    }

    public static byte[] encodeKey(String key) {
        byte[] bytes = key.getBytes(UTF_8);
        return new ByteArrayBuilder(bytes.length + 1)
                .append(bytes)
                .append(NULL_BYTE)
                .build();
    }

    public static byte[] encodeValue(Value value) {
        return FUNCTIONS.get(value.type()).apply(value);
    }
}
