package store.common.io;

import store.common.value.Value;

enum BinaryType {

    NULL(0x01),
    BYTE_ARRAY(0x02),
    BOOLEAN(0x03),
    BYTE(0x04),
    INTEGER(0x05),
    LONG(0x06),
    BIG_DECIMAL(0x07),
    STRING(0x08),
    DATE(0x09),
    MAP(0x0A),
    LIST(0x0B);
    public final byte value;

    private BinaryType(int value) {
        this.value = (byte) value;
    }

    public static BinaryType of(Value value) {
        switch (value.type()) {
            case NULL:
                return NULL;
            case BYTE_ARRAY:
                return BYTE_ARRAY;
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return BYTE;
            case INTEGER:
                return INTEGER;
            case LONG:
                return LONG;
            case BIG_DECIMAL:
                return BIG_DECIMAL;
            case STRING:
                return STRING;
            case DATE:
                return DATE;
            case MAP:
                return MAP;
            case LIST:
                return LIST;
            default:
                throw new IllegalArgumentException(value.type().toString());
        }
    }

    public static BinaryType of(byte value) {
        for (BinaryType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.valueOf(value));
    }
}
