package store.common.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Computes Hash and length from a given input.
 */
public class Digest {

    private final Hash hash;
    private final Long length;

    private Digest(Hash hash, Long length) {
        this.hash = hash;
        this.length = length;
    }

    /**
     * @return Computed hash.
     */
    public Hash getHash() {
        return hash;
    }

    /**
     * @return Computed length.
     */
    public Long getLength() {
        return length;
    }

    /**
     * Builder.
     */
    public static class DigestBuilder {

        private static final String ALGORITHM = "SHA";
        private final MessageDigest messageDigest;
        private long totalLength = 0;

        /**
         * Constructor.
         */
        public DigestBuilder() {
            try {
                messageDigest = MessageDigest.getInstance(ALGORITHM);

            } catch (NoSuchAlgorithmException e) {
                // Actually impossible.
                throw new AssertionError(e);
            }
        }

        /**
         * Add data to computed digest.
         *
         * @param bytes Input bytes array.
         * @param length Number of bytes to read from supplied array (no offset).
         * @return This builder instance.
         */
        public DigestBuilder add(byte[] bytes, int length) {
            messageDigest.update(bytes, 0, length);
            totalLength += length;
            return this;
        }

        /**
         * Build.
         *
         * @return A new Digest instance.
         */
        public Digest build() {
            return new Digest(new Hash(messageDigest.digest()), totalLength);
        }
    }
}
