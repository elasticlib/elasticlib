package store.common.io;

import static com.google.common.base.Charsets.UTF_8;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static store.common.io.BinaryConstants.FALSE;
import static store.common.io.BinaryConstants.NULL_BYTE;
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

class Decoder {

    private final byte[] bytes;
    private int index;

    public Decoder(byte[] bytes) {
        this.bytes = bytes;
        index = 0;
    }

    public Map<String, Value> decode() {
        return decodeMap(bytes.length);
    }

    private byte read() {
        byte b = bytes[index];
        index++;
        return b;
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

    private Value decodeValue(ValueType type) {
        switch (type) {
            case NULL:
                return Value.ofNull();
            case BYTE_ARRAY:
                return Value.of(decodeByteArray());
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
                return Value.of(decodeMap(decodeInt()));
            case LIST:
                return Value.of(decodeList());
            default:
                throw new IllegalArgumentException(type.toString());
        }
    }

    private byte[] decodeByteArray() {
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
        return new String(decodeByteArray(), UTF_8);
    }

    private Date decodeDate() {
        return new Date(decodeLong());
    }

    private Map<String, Value> decodeMap(int size) {
        Map<String, Value> map = new LinkedHashMap<>();
        int limit = index + size;
        while (index < limit) {
            ValueType type = decodeType(read());
            String key = decodeKey();
            map.put(key, decodeValue(type));
        }
        return map;
    }

    private List<Value> decodeList() {
        List<Value> list = new ArrayList<>();
        int limit = index + decodeInt();
        while (index < limit) {
            ValueType type = decodeType(read());
            list.add(decodeValue(type));
        }
        return list;
    }
}
