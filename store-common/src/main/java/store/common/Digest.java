package store.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Digest {

    private final Hash hash;
    private final Long length;

    private Digest(Hash hash, Long length) {
        this.hash = hash;
        this.length = length;
    }

    public Hash getHash() {
        return hash;
    }

    public Long getLength() {
        return length;
    }

    static class DigestBuilder {

        private static final String ALGORITHM = "SHA";
        private final MessageDigest messageDigest;
        private long totalLength = 0;

        public DigestBuilder() {
            try {
                messageDigest = MessageDigest.getInstance(ALGORITHM);

            } catch (NoSuchAlgorithmException e) {
                // Actually impossible.
                throw new AssertionError(e);
            }
        }

        public DigestBuilder add(byte[] bytes, int length) {
            messageDigest.update(bytes, 0, length);
            totalLength += length;
            return this;
        }

        public Digest build() {
            return new Digest(new Hash(messageDigest.digest()), totalLength);
        }
    }
}
