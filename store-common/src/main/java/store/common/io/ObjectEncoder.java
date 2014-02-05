package store.common.io;

import static com.google.common.base.Charsets.UTF_8;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import static store.common.io.BinaryConstants.*;
import static store.common.io.BinaryType.*;
import store.common.value.Value;

/**
 * A JSON-like binary encoder with a fluent API.
 * <p>
 * Serialize a sequence of key-value pairs into a JSON-like binary document. Produced format supports the embedding of
 * documents and arrays within other documents and arrays. It also contains extensions that allow representation of data
 * types that are not part of the JSON spec.
 *
 * @see ObjectDecoder
 */
public final class ObjectEncoder {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    ByteArrayBuilder arrayBuilder = new ByteArrayBuilder();

    /**
     * Add a null value to the binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @return This encoder instance.
     */
    public ObjectEncoder putNull(String key) {
        arrayBuilder.append(NULL.value)
                .append(encodeKey(key));
        return this;
    }

    /**
     * Add a byte array to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, byte[] value) {
        arrayBuilder.append(BYTE_ARRAY.value)
                .append(encodeKey(key))
                .append(encodeRaw(value));
        return this;
    }

    /**
     * Add a boolean to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, boolean value) {
        arrayBuilder.append(BOOLEAN.value)
                .append(encodeKey(key))
                .append(encodeBoolean(value));
        return this;
    }

    /**
     * Add a byte to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, byte value) {
        arrayBuilder.append(BYTE.value)
                .append(encodeKey(key))
                .append(encodeByte(value));
        return this;
    }

    /**
     * Add an integer to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, int value) {
        arrayBuilder.append(INTEGER.value)
                .append(encodeKey(key))
                .append(encodeInt(value));
        return this;
    }

    /**
     * Add a long to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, long value) {
        arrayBuilder.append(LONG.value)
                .append(encodeKey(key))
                .append(encodeLong(value));
        return this;
    }

    /**
     * Add a string to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, String value) {
        arrayBuilder.append(STRING.value)
                .append(encodeKey(key))
                .append(encodeString(value));
        return this;
    }

    /**
     * Add a big decimal to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, BigDecimal value) {
        arrayBuilder.append(BIG_DECIMAL.value)
                .append(encodeKey(key))
                .append(encodeBigDecimal(value));
        return this;
    }

    /**
     * Add a date to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, Date value) {
        arrayBuilder.append(DATE.value)
                .append(encodeKey(key))
                .append(encodeDate(value));
        return this;
    }

    /**
     * Add a map to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, Map<String, Value> value) {
        arrayBuilder.append(MAP.value)
                .append(encodeKey(key))
                .append(encodeMap(value));
        return this;
    }

    /**
     * Add a list to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, List<Value> value) {
        arrayBuilder.append(LIST.value)
                .append(encodeKey(key))
                .append(encodeList(value));
        return this;
    }

    /**
     * Add a value to binary structure to build.
     *
     * @param key Key with which the supplied value is to be associated.
     * @param value Value to be associated with the supplied key.
     * @return This encoder instance.
     */
    public ObjectEncoder put(String key, Value value) {
        arrayBuilder.append(BinaryType.of(value).value)
                .append(encodeKey(key))
                .append(encodeValue(value));
        return this;
    }

    /**
     * Build the binary structure.
     *
     * @return Built structure as a byte array.
     */
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

    private static byte[] encodeByte(byte value) {
        return new byte[]{value};
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

    private static byte[] encodeBigDecimal(BigDecimal value) {
        return encodeString(value.toString());
    }

    private static byte[] encodeString(String value) {
        return encodeRaw(value.getBytes(UTF_8));
    }

    private static byte[] encodeDate(Date value) {
        return encodeLong(value.getTime());
    }

    private static byte[] encodeMap(Map<String, Value> value) {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        for (Entry<String, Value> entry : value.entrySet()) {
            builder.append(BinaryType.of(entry.getValue()).value)
                    .append(encodeKey(entry.getKey()))
                    .append(encodeValue(entry.getValue()));
        }
        return builder.prependSizeAndBuild();
    }

    private static byte[] encodeList(List<Value> value) {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        for (Value item : value) {
            builder.append(BinaryType.of(item).value)
                    .append(encodeValue(item));
        }
        return builder.prependSizeAndBuild();
    }

    private static byte[] encodeValue(Value value) {
        switch (value.type()) {
            case NULL:
                return EMPTY_BYTE_ARRAY;
            case BYTE_ARRAY:
                return encodeRaw(value.asByteArray());
            case BOOLEAN:
                return encodeBoolean(value.asBoolean());
            case BYTE:
                return encodeByte(value.asByte());
            case INTEGER:
                return encodeInt(value.asInt());
            case LONG:
                return encodeLong(value.asLong());
            case BIG_DECIMAL:
                return encodeBigDecimal(value.asBigDecimal());
            case STRING:
                return encodeString(value.asString());
            case DATE:
                return encodeDate(value.asDate());
            case MAP:
                return encodeMap(value.asMap());
            case LIST:
                return encodeList(value.asList());
            default:
                throw new IllegalArgumentException(value.type().toString());
        }
    }
}
