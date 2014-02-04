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

public class ObjectDecoder {

    private final Map<String, Value> map;

    public ObjectDecoder(byte[] bytes) {
        map = unmodifiableMap(new Decoder(bytes).decode());
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public Set<String> keyset() {
        return map.keySet();
    }

    public byte[] getRaw(String key) {
        return get(key).asByteArray();
    }

    public boolean getBoolean(String key) {
        return get(key).asBoolean();
    }

    public byte getByte(String key) {
        return get(key).asByte();
    }

    public int getInt(String key) {
        return get(key).asInt();
    }

    public long getLong(String key) {
        return get(key).asLong();
    }

    public BigDecimal getBigDecimal(String key) {
        return get(key).asBigDecimal();
    }

    public String getString(String key) {
        return get(key).asString();
    }

    public Date getDate(String key) {
        return get(key).asDate();
    }

    public Map<String, Value> getMap(String key) {
        return get(key).asMap();
    }

    public List<Value> getList(String key) {
        return get(key).asList();
    }

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
