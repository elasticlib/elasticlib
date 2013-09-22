package store.server.operation;

enum OpCode {

    BEGIN_PUT(0x01),
    END_PUT(0x02),
    BEGIN_DELETE(0x03),
    END_DELETE(0x04);
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
