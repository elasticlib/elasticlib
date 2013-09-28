package store.server.history;

enum Operation {

    PUT(0x01),
    DELETE(0x02);
    private final byte value;

    private Operation(int value) {
        this.value = (byte) value;
    }

    public static Operation of(byte value) {
        return null; // TODO
    }

    public byte value() {
        return value;
    }
}
