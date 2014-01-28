package store.common.io;

enum BinaryType {

    RAW(0x01),
    BOOLEAN(0x02),
    BYTE(0x03),
    INTEGER(0x04),
    LONG(0x05),
    STRING(0x06),
    DATE(0x07),
    MAP(0x08),
    LIST(0x09);
    public final byte value;

    private BinaryType(int value) {
        this.value = (byte) value;
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
