package store.common.io;

import static com.google.common.base.Charsets.UTF_8;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.unmodifiableMap;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import static store.common.io.BinaryConstants.*;
import store.common.value.Value;

/**
 * A binary decoder. Converts a JSON-like binary structure into a map.
 *
 * @see ObjectEncoder
 */
public class ObjectDecoder {

    private final Map<String, Value> map;

    /**
     * Constructor.
     *
     * @param bytes A JSON-like binary structure.
     */
    public ObjectDecoder(byte[] bytes) {
        map = unmodifiableMap(new Decoder(bytes).decode());
    }

    /**
     * Returns <tt>true</tt> if decoded structure contains a mapping for the specified key.
     *
     * @param key key whose presence in decoded structure is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified key
     */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /**
     * Returns a sorted {@link Set} of the keys contained in decoded structure.
     *
     * @return A set.
     */
    public Set<String> keyset() {
        return map.keySet();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a byte array.
     *
     * @param key The key whose associated value is to be returned.
     * @return A byte array.
     */
    public byte[] getRaw(String key) {
        return get(key).asByteArray();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a boolean.
     *
     * @param key The key whose associated value is to be returned.
     * @return A boolean.
     */
    public boolean getBoolean(String key) {
        return get(key).asBoolean();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a byte.
     *
     * @param key The key whose associated value is to be returned.
     * @return A byte.
     */
    public byte getByte(String key) {
        return get(key).asByte();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not an integer.
     *
     * @param key The key whose associated value is to be returned.
     * @return An integer.
     */
    public int getInt(String key) {
        return get(key).asInt();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a long.
     *
     * @param key The key whose associated value is to be returned.
     * @return A long.
     */
    public long getLong(String key) {
        return get(key).asLong();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a big decimal.
     *
     * @param key The key whose associated value is to be returned.
     * @return A big decimal.
     */
    public BigDecimal getBigDecimal(String key) {
        return get(key).asBigDecimal();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a string.
     *
     * @param key The key whose associated value is to be returned.
     * @return A string.
     */
    public String getString(String key) {
        return get(key).asString();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a date.
     *
     * @param key The key whose associated value is to be returned.
     * @return A date.
     */
    public Date getDate(String key) {
        return get(key).asDate();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a map.
     *
     * @param key The key whose associated value is to be returned.
     * @return A map.
     */
    public Map<String, Value> getMap(String key) {
        return get(key).asMap();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key or if the value is actually not a list.
     *
     * @param key The key whose associated value is to be returned.
     * @return A list.
     */
    public List<Value> getList(String key) {
        return get(key).asList();
    }

    /**
     * Returns the value to which the specified key is mapped in decoded structure. Fails if decoded structure does not
     * contain a mapping for the specified key.
     *
     * @param key The key whose associated value is to be returned.
     * @return A Value.
     */
    public Value get(String key) {
        if (!map.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return map.get(key);
    }

    private static class Decoder {

        private final byte[] bytes;
        private int index;

        public Decoder(byte[] bytes) {
            this.bytes = bytes;
        }

        public Map<String, Value> decode() {
            Map<String, Value> map = new LinkedHashMap<>();
            index = 0;
            while (index < bytes.length) {
                BinaryType type = decodeBinaryType();
                String key = decodeKey();
                map.put(key, decodeValue(type));
            }
            return map;
        }

        private byte read() {
            byte b = bytes[index];
            index++;
            return b;
        }

        BinaryType decodeBinaryType() {
            return BinaryType.of(read());
        }

        private String decodeKey() {
            for (int i = index; i < bytes.length; i++) {
                if (bytes[i] == NULL_BYTE) {
                    String key = new String(bytes, index, i - index, UTF_8);
                    index = i + 1;
                    return key;
                }
            }
            throw new IllegalArgumentException();
        }

        private Value decodeValue(BinaryType type) {
            switch (type) {
                case NULL:
                    return Value.ofNull();
                case BYTE_ARRAY:
                    return Value.of(decodeRaw());
                case BOOLEAN:
                    return Value.of(decodeBoolean());
                case BYTE:
                    return Value.of(decodeByte());
                case INTEGER:
                    return Value.of(decodeInt());
                case LONG:
                    return Value.of(decodeLong());
                case BIG_DECIMAL:
                    return Value.of(decodeBigDecimal());
                case STRING:
                    return Value.of(decodeString());
                case DATE:
                    return Value.of(decodeDate());
                case MAP:
                    return Value.of(decodeMap());
                case LIST:
                    return Value.of(decodeList());
                default:
                    throw new IllegalArgumentException(type.toString());
            }
        }

        private byte[] decodeRaw() {
            int length = decodeInt();
            byte[] value = Arrays.copyOfRange(bytes, index, index + length);
            index += length;
            return value;
        }

        private boolean decodeBoolean() {
            switch (read()) {
                case FALSE:
                    return false;
                case TRUE:
                    return true;
                default:
                    throw new IllegalArgumentException();
            }
        }

        private byte decodeByte() {
            return read();
        }

        private int decodeInt() {
            int value = ByteBuffer.wrap(bytes, index, 4)
                    .getInt();
            index += 4;
            return value;
        }

        private long decodeLong() {
            long value = ByteBuffer.wrap(bytes, index, 8)
                    .getLong();
            index += 8;
            return value;
        }

        private BigDecimal decodeBigDecimal() {
            return new BigDecimal(decodeString());
        }

        private String decodeString() {
            return new String(decodeRaw(), UTF_8);
        }

        private Date decodeDate() {
            return new Date(decodeLong());
        }

        private Map<String, Value> decodeMap() {
            Map<String, Value> map = new LinkedHashMap<>();
            int limit = index + decodeInt();
            while (index < limit) {
                BinaryType type = decodeBinaryType();
                String key = decodeKey();
                map.put(key, decodeValue(type));
            }
            return map;
        }

        private List<Value> decodeList() {
            List<Value> list = new ArrayList<>();
            int limit = index + decodeInt();
            while (index < limit) {
                BinaryType type = decodeBinaryType();
                list.add(decodeValue(type));
            }
            return list;
        }
    }
}
