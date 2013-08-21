package store.io;

enum BinaryType {

    RAW(0x01),
    BOOLEAN(0x02),
    INTEGER(0x03),
    LONG(0x04),
    STRING(0x05),
    DATE(0x06),
    MAP(0x07),
    LIST(0x08);
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
