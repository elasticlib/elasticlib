package store.server.operation;

enum OpCode {

    PUT(0x01),
    DELETE(0x02);
    private final byte value;

    private OpCode(int value) {
        this.value = (byte) value;
    }

    public static OpCode of(byte value) {
        return null; // TODO
    }

    public byte value() {
        return value;
    }
}
