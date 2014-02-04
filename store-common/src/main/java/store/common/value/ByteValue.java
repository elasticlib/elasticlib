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
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + type().hashCode();
        hash = 67 * hash + value;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ByteValue other = (ByteValue) obj;
        return value == other.value;
    }
}
