package store.common.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Hash and length of a given content.
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
     * Builder. Compute a digest from a given input.
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
         * @return the total number of previously added bytes.
         */
        public long getLength() {
            return totalLength;
        }

        /**
         * @return The Hash of previously added bytes.
         */
        public Hash getHash() {
            try {
                MessageDigest clone = (MessageDigest) messageDigest.clone();
                return new Hash(clone.digest());

            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }

        /**
         * Create a new digest. Does not affect this builder state, so more data may be added after this operation in
         * order to build other digests.
         *
         * @return A new Digest instance.
         */
        public Digest build() {
            return new Digest(getHash(), getLength());
        }
    }
}
