package store.common.value;

class ByteValue extends Value {

    private final byte value;

    public ByteValue(byte value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.BYTE;
    }

    @Override
    public byte asByte() {
        return value;
    }

    @Override
    public String asHexadecimalString() {
        return String.format("%02x", value);
    }

    @Override
    public String toString() {
        return "0x" + asHexadecimalString();
    }

    @Override
    Object value() {
        return value;
    }
}
