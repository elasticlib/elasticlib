package store.server.io;

import static com.google.common.base.Charsets.UTF_8;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static store.server.io.BinaryConstants.*;
import static store.server.io.BinaryType.*;

public final class ObjectEncoder {

    ByteArrayBuilder arrayBuilder = new ByteArrayBuilder();

    private ObjectEncoder() {
    }

    public static ObjectEncoder encoder() {
        return new ObjectEncoder();
    }

    public ObjectEncoder put(String key, byte[] value) {
        arrayBuilder.append(RAW.value)
                .append(encodeKey(key))
                .append(encodeRaw(value));
        return this;
    }

    public ObjectEncoder put(String key, boolean value) {
        arrayBuilder.append(BOOLEAN.value)
                .append(encodeKey(key))
                .append(encodeBoolean(value));
        return this;
    }

    public ObjectEncoder put(String key, int value) {
        arrayBuilder.append(INTEGER.value)
                .append(encodeKey(key))
                .append(encodeInt(value));
        return this;
    }

    public ObjectEncoder put(String key, long value) {
        arrayBuilder.append(LONG.value)
                .append(encodeKey(key))
                .append(encodeLong(value));
        return this;
    }

    public ObjectEncoder put(String key, String value) {
        arrayBuilder.append(STRING.value)
                .append(encodeKey(key))
                .append(encodeString(value));
        return this;
    }

    public ObjectEncoder put(String key, Date value) {
        arrayBuilder.append(DATE.value)
                .append(encodeKey(key))
                .append(encodeDate(value));
        return this;
    }

    public ObjectEncoder put(String key, Map<String, ?> value) {
        arrayBuilder.append(MAP.value)
                .append(encodeKey(key))
                .append(encodeMap(value));
        return this;
    }

    public ObjectEncoder put(String key, List<?> value) {
        arrayBuilder.append(LIST.value)
                .append(encodeKey(key))
                .append(encodeList(value));
        return this;
    }

    public byte[] build() {
        return arrayBuilder.prependSizeAndBuild();
    }

    private static byte[] encodeKey(String key) {
        byte[] bytes = key.getBytes(UTF_8);
        return new ByteArrayBuilder(bytes.length + 1)
                .append(bytes)
                .append(NULL_BYTE)
                .build();
    }

    private static byte[] encodeRaw(byte[] value) {
        return new ByteArrayBuilder(value.length + 4)
                .append(encodeInt(value.length))
                .append(value)
                .build();
    }

    private static byte[] encodeBoolean(boolean value) {
        return new byte[]{value ? TRUE : FALSE};
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
        return encodeRaw(value.getBytes(UTF_8));
    }

    private static byte[] encodeDate(Date value) {
        return encodeLong(value.getTime());
    }

    private static byte[] encodeMap(Map<String, ?> value) {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        for (Entry<String, ?> entry : value.entrySet()) {
            builder.append(type(entry.getValue()).value)
                    .append(encodeKey(entry.getKey()))
                    .append(encodeValue(entry.getValue()));
        }
        return builder.prependSizeAndBuild();
    }

    private static byte[] encodeList(List<?> value) {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        for (Object obj : value) {
            builder.append(type(obj).value)
                    .append(encodeValue(obj));
        }
        return builder.prependSizeAndBuild();
    }

    @SuppressWarnings("unchecked")
    private static byte[] encodeValue(Object value) {
        if (value instanceof byte[]) {
            return encodeRaw((byte[]) value);
        }
        if (value instanceof Boolean) {
            return encodeBoolean((boolean) value);
        }
        if (value instanceof Integer) {
            return encodeInt((int) value);
        }
        if (value instanceof Long) {
            return encodeLong((long) value);
        }
        if (value instanceof String) {
            return encodeString((String) value);
        }
        if (value instanceof Date) {
            return encodeDate((Date) value);
        }
        if (value instanceof Map) {
            return encodeMap((Map<String, ?>) value);
        }
        if (value instanceof List) {
            return encodeList((List) value);
        }
        throw new IllegalArgumentException(value.getClass()
                .toString());
    }

    private static BinaryType type(Object value) {
        if (value instanceof byte[]) {
            return RAW;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof String) {
            return STRING;
        }
        if (value instanceof Date) {
            return DATE;
        }
        if (value instanceof Map) {
            return MAP;
        }
        if (value instanceof List) {
            return LIST;
        }
        throw new IllegalArgumentException(value.getClass()
                .toString());
    }
}
