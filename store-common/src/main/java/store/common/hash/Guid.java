package store.common.hash;

import java.security.SecureRandom;

/**
 * Represents a globally unique identifier.
 */
public class Guid extends AbstractKey implements Comparable<Guid> {

    private static final SecureRandom GENERATOR = new SecureRandom();

    /**
     * Byte array based constructor.
     *
     * @param bytes A byte array.
     */
    public Guid(byte[] bytes) {
        super(bytes);
    }

    /**
     * Hexadecimal string based constructor.
     *
     * @param hexadecimal Hexadecimal encoded bytes. Case unsensitive.
     */
    public Guid(String hexadecimal) {
        super(hexadecimal);
    }

    /**
     * Ramdomly generates a globally unique identifier. Thread-safe.
     *
     * @return A new GUID instance.
     */
    public static synchronized Guid random() {
        // Implementation note : Not sure that GUID are security-sensitive, however we use SecureRandom here,
        // expecting stronger Randomness.
        // The SecureRandom class is likely to be thread-safe, but this is not explicitely stated in its documentation.
        // Therefore this method is synchronized to ensure this. Contention might happen, but this does not matter
        // as this method is not expected to intensively used.

        byte[] randomBytes = new byte[16];
        GENERATOR.nextBytes(randomBytes);
        return new Guid(randomBytes);
    }

    @Override
    public int compareTo(Guid that) {
        return super.compareTo(that);
    }
}
