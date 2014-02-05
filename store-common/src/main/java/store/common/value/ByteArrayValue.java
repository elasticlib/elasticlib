package store.common.value;

import static com.google.common.io.BaseEncoding.base16;
import java.util.Arrays;

/**
 * Represents a byte array value
 */
public final class ByteArrayValue extends Value {

    private final byte[] value;

    /**
     * Constructor.
     *
     * @param value Actual wrapped value.
     */
    public ByteArrayValue(byte[] value) {
        this.value = Arrays.copyOf(value, value.length);
    }

    @Override
    public ValueType type() {
        return ValueType.BYTE_ARRAY;
    }

    @Override
    public byte[] asByteArray() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public String asHexadecimalString() {
        return base16()
                .lowerCase()
                .encode(value);
    }

    @Override
    public String toString() {
        return asHexadecimalString();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 19 * hash + type().hashCode();
        hash = 19 * hash + Arrays.hashCode(value);
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
        ByteArrayValue other = (ByteArrayValue) obj;
        return Arrays.equals(value, other.value);
    }

    @Override
    Object value() {
        throw new UnsupportedOperationException();
    }
}
