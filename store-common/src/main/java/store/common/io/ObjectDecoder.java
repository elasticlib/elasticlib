package store.common.io;

import static com.google.common.base.Charsets.UTF_8;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static store.common.io.BinaryConstants.*;
import static store.common.io.BinaryType.RAW;

public class ObjectDecoder {

    private final Map<String, Object> map;

    public ObjectDecoder(byte[] bytes) {
        map = new Decoder(bytes).decode();
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public byte[] getRaw(String key) {
        return (byte[]) map.get(key);
    }

    public boolean getBoolean(String key) {
        return (boolean) map.get(key);
    }

    public byte getByte(String key) {
        return (byte) map.get(key);
    }

    public int getInt(String key) {
        return (int) map.get(key);
    }

    public long getLong(String key) {
        return (long) map.get(key);
    }

    public String getString(String key) {
        return (String) map.get(key);
    }

    public Date getDate(String key) {
        return (Date) map.get(key);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        return (Map<String, Object>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    public List<Object> getList(String key) {
        return (List<Object>) map.get(key);
    }

    private static class Decoder {

        private final byte[] bytes;
        private int index;

        public Decoder(byte[] bytes) {
            this.bytes = bytes;
        }

        public Map<String, Object> decode() {
            Map<String, Object> map = new HashMap<>();
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

        private Object decodeValue(BinaryType type) {
            switch (type) {
                case RAW:
                    return decodeRaw();

                case BOOLEAN:
                    return decodeBoolean();
                case BYTE:
                    return decodeByte();

                case INTEGER:
                    return decodeInt();

                case LONG:
                    return decodeLong();

                case STRING:
                    return decodeString();

                case DATE:
                    return decodeDate();

                case MAP:
                    return decodeMap();

                case LIST:
                    return decodeList();

                default:
                    throw new RuntimeException();
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

        private String decodeString() {
            return new String(decodeRaw(), UTF_8);
        }

        private Date decodeDate() {
            return new Date(decodeLong());
        }

        private Map<String, ?> decodeMap() {
            Map<String, Object> map = new HashMap<>();
            int limit = index + decodeInt();
            while (index < limit) {
                BinaryType type = decodeBinaryType();
                String key = decodeKey();
                map.put(key, decodeValue(type));
            }
            return map;
        }

        private List<?> decodeList() {
            List<Object> list = new ArrayList<>();
            int limit = index + decodeInt();
            while (index < limit) {
                BinaryType type = decodeBinaryType();
                list.add(decodeValue(type));
            }
            return list;
        }
    }
}
