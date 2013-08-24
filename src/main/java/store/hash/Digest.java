package store.hash;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import store.exception.StoreRuntimeException;

public class Digest {

    private static final int BUFFER_SIZE = 65536;
    private static final String ALGORITHM = "SHA";
    private final Hash hash;
    private final Long length;

    public Digest(Hash hash, Long length) {
        this.hash = hash;
        this.length = length;
    }

    public Hash getHash() {
        return hash;
    }

    public Long getLength() {
        return length;
    }

    public static Digest of(InputStream inputStream) {
        try {
            DigestBuilder builder = new DigestBuilder();

            byte[] buffer = new byte[BUFFER_SIZE];
            int len = inputStream.read(buffer);
            while (len != -1) {
                builder.add(buffer, len);
                len = inputStream.read(buffer);
            }
            return builder.build();

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static class DigestBuilder {

        private final MessageDigest messageDigest;
        private long totalLength = 0;

        public DigestBuilder() {
            try {
                messageDigest = MessageDigest.getInstance(ALGORITHM);

            } catch (NoSuchAlgorithmException e) {
                throw new StoreRuntimeException(e);
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
