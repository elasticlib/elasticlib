package store.common.bson;

import static com.google.common.base.Charsets.UTF_8;
import com.google.common.base.Function;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import static store.common.bson.BinaryConstants.*;
import store.common.value.Value;
import store.common.value.ValueType;
import static store.common.value.ValueType.ARRAY;
import static store.common.value.ValueType.BINARY;
import static store.common.value.ValueType.BOOLEAN;
import static store.common.value.ValueType.DATE;
import static store.common.value.ValueType.DECIMAL;
import static store.common.value.ValueType.HASH;
import static store.common.value.ValueType.INTEGER;
import static store.common.value.ValueType.NULL;
import static store.common.value.ValueType.OBJECT;
import static store.common.value.ValueType.STRING;

final class ValueWriting {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Map<ValueType, Function<Value, byte[]>> WRITERS = new EnumMap<>(ValueType.class);

    static {
        WRITERS.put(NULL, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return EMPTY_BYTE_ARRAY;
            }
        });
        WRITERS.put(HASH, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return value.asHash().getBytes();
            }
        });
        WRITERS.put(BINARY, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return writeByteArray(value.asByteArray());
            }
        });
        WRITERS.put(BOOLEAN, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return new byte[]{value.asBoolean() ? TRUE : FALSE};
            }
        });
        WRITERS.put(INTEGER, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return writeLong(value.asLong());
            }
        });
        WRITERS.put(DECIMAL, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return writeString(value.toString());
            }
        });
        WRITERS.put(STRING, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return writeString(value.asString());
            }
        });
        WRITERS.put(DATE, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                return writeLong(value.asInstant().getMillis());
            }
        });
        WRITERS.put(OBJECT, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                ByteArrayBuilder builder = new ByteArrayBuilder();
                for (Entry<String, Value> entry : value.asMap().entrySet()) {
                    builder.append(writeType(entry.getValue().type()))
                            .append(writeKey(entry.getKey()))
                            .append(writeValue(entry.getValue()));
                }
                return builder.prependSizeAndBuild();
            }
        });
        WRITERS.put(ARRAY, new Function<Value, byte[]>() {
            @Override
            public byte[] apply(Value value) {
                ByteArrayBuilder builder = new ByteArrayBuilder();
                for (Value item : value.asList()) {
                    builder.append(writeType(item.type()))
                            .append(writeValue(item));
                }
                return builder.prependSizeAndBuild();
            }
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
