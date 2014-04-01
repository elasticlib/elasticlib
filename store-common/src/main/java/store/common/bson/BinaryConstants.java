package store.common.bson;

import com.google.common.collect.BiMap;
import com.google.common.collect.EnumHashBiMap;
import store.common.value.ValueType;
import static store.common.value.ValueType.*;

final class BinaryConstants {

    public static final byte TRUE = 0x01;
    public static final byte FALSE = 0x00;
    public static final byte NULL_BYTE = 0x00;
    private static final BiMap<ValueType, Byte> TYPES = EnumHashBiMap.create(ValueType.class);

    static {
        map(NULL, 0x01);
        map(BINARY, 0x02);
        map(BOOLEAN, 0x03);
        map(INTEGER, 0x04);
        map(DECIMAL, 0x05);
        map(STRING, 0x06);
        map(DATE, 0x07);
        map(OBJECT, 0x08);
        map(ARRAY, 0x09);
    }

    private BinaryConstants() {
        // Non-instanciable
    }

    private static void map(ValueType key, int value) {
        TYPES.put(key, (byte) value);
    }

    public static byte writeType(ValueType type) {
        return TYPES.get(type);
    }

    public static ValueType readType(byte b) {
        return TYPES.inverse().get(b);
    }
}
