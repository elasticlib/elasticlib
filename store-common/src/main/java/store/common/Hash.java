package store.common;

import static com.google.common.io.BaseEncoding.base16;
import java.util.Arrays;

/**
 * Represents a Hash.
 * <p>
 * Wraps bare bytes and provides additionnal operations.<br>
 * Is comparable in order to sort hashes in ascending lexicographical order over their encoded form.
 */
public final class Hash implements Comparable<Hash> {

    private final byte[] bytes;

    /**
     * Byte array based constructor.
     *
     * @param bytes A byte array.
     */
    public Hash(byte[] bytes) {
        this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Hexadecimal string based constructor.
     *
     * @param encoded Hexadecimal encoded bytes. Case unsensitive.
     */
    public Hash(String encoded) {
        this(base16()
                .lowerCase()
                .decode(encoded.toLowerCase()));
    }

    /**
     * @return This hash as a byte array.
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * @return This hash encoded as an hexadecimal lower-case string.
     */
    public String encode() {
        return base16()
                .lowerCase()
                .encode(bytes);
    }

    @Override
    public String toString() {
        return encode();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(Hash that) {
        return encode().compareTo(that.encode());
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return Arrays.equals(bytes, Hash.class.cast(obj).bytes);
    }
}
