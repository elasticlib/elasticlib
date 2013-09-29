package store.common;

public enum Operation {

    PUT(0x01),
    DELETE(0x02);
    private final byte value;

    private Operation(int value) {
        this.value = (byte) value;
    }

    public static Operation of(byte value) {
        for (Operation operation : values()) {
            if (operation.value == value) {
                return operation;
            }
        }
        throw new IllegalArgumentException("0x" + Integer.toHexString(value));
    }

    public byte value() {
        return value;
    }

    @Override
    public String toString() {
        return name().charAt(0) + name().substring(1).toLowerCase();
    }
}
