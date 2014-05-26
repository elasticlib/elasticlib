package store.common.hash;

import java.util.Set;
import java.util.TreeSet;

/**
 * Represents a Hash.
 * <p>
 * Wraps bare bytes and provides additionnal operations.<br>
 * Is comparable in order to sort hashes in ascending lexicographical order over their encoded form.
 */
public final class Hash extends AbstractKey implements Comparable<Hash> {

    /**
     * Byte array based constructor.
     *
     * @param bytes A byte array.
     */
    public Hash(byte[] bytes) {
        super(bytes);
    }

    /**
     * Hexadecimal string based constructor.
     *
     * @param hexadecimal Hexadecimal encoded bytes. Case unsensitive.
     */
    public Hash(String hexadecimal) {
        super(hexadecimal);
    }

    /**
     * Derives a key from this hash.
     *
     * @param length Key length in characters. Expected to be positive and less than or equal to encoded hash length.
     * @return This hash encoded and truncated to supplied length.
     */
    public String key(int length) {
        return asHexadecimalString().substring(0, length);
    }

    /**
     * Generates the set of all derivable keys for supplied length. The size of produced set is equal to 16 raised to
     * the power of supplied length which quickly becomes excessive. Therefore, This will fail if supplied length if
     * more than or equal to 6.
     *
     *
     * @param keyLength Key length in characters
     * @return A set of keys.
     */
    public static Set<String> keySet(int keyLength) {
        if (keyLength < 0 || keyLength >= 6) {
            throw new IllegalArgumentException();
        }
        int size = size(keyLength);
        Set<String> keySet = new TreeSet<>();
        for (int i = 0; i < size; i++) {
            keySet.add(keyOf(i, keyLength));
        }
        return keySet;
    }

    private static int size(int keyLength) {
        return keyLength == 0 ? 0 : 1 << (4 * keyLength);
    }

    private static String keyOf(int index, int keyLength) {
        String hex = Integer.toHexString(index);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < keyLength - hex.length(); i++) {
            builder.append('0');
        }
        return builder.append(hex).toString();
    }

    @Override
    public int compareTo(Hash that) {
        return super.compareTo(that);
    }
}
