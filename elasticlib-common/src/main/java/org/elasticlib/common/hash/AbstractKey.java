package org.elasticlib.common.hash;

import static com.google.common.io.BaseEncoding.base16;
import java.util.Arrays;

/**
 * Represents a key.
 * <p>
 * Wraps bare bytes and provides additionnal operations.
 */
abstract class AbstractKey {

    private final byte[] bytes;

    protected AbstractKey(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    protected AbstractKey(String hexadecimal) {
        this(base16()
                .lowerCase()
                .decode(hexadecimal.toLowerCase()));
    }

    /**
     * @return This key as a byte array.
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * @return This key encoded as an hexadecimal lower-case string.
     */
    public String asHexadecimalString() {
        return base16()
                .lowerCase()
                .encode(bytes);
    }

    @Override
    public String toString() {
        return asHexadecimalString();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        // Ensure that instances of two different derived classes are never equal.
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        AbstractKey other = (AbstractKey) obj;
        return Arrays.equals(bytes, other.bytes);
    }

    protected static boolean isBase16(String value) {
        return value.matches("[0-9a-fA-F]*");
    }

    // Helps to implements Comparable in derived classes.
    protected int compareToImpl(AbstractKey that) {
        return asHexadecimalString().compareTo(that.asHexadecimalString());
    }
}
