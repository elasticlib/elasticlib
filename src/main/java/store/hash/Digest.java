package store.hash;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
            MessageDigest messageDigest = MessageDigest.getInstance(ALGORITHM);
            long totalLength = 0;

            byte[] buffer = new byte[BUFFER_SIZE];
            int len = inputStream.read(buffer);
            while (len != -1) {
                messageDigest.update(buffer);
                totalLength += len;
                len = inputStream.read(buffer);
            }
            return new Digest(new Hash(messageDigest.digest()), totalLength);

        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
