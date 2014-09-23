package store.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import store.common.hash.Digest;
import store.common.hash.Digest.DigestBuilder;

/**
 * IO utilities.
 */
public final class IoUtil {

    private static final int BUFFER_SIZE = 8192;

    private IoUtil() {
    }

    /**
     * Writes all bytes read from input-stream to output-stream.
     *
     * @param inputStream Source input-stream.
     * @param outputStream Destination output-stream.
     * @throws IOException If an IO error happens.
     */
    public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int len = inputStream.read(buffer);
        while (len != -1) {
            outputStream.write(buffer, 0, len);
            len = inputStream.read(buffer);
        }
    }

    /**
     * Writes all bytes read from input-stream to output-stream, and provides a digest of these bytes.
     *
     * @param inputStream Source input-stream.
     * @param outputStream Destination output-stream.
     * @return A digest of copied bytes.
     * @throws IOException If an IO error happens.
     */
    public static Digest copyAndDigest(InputStream inputStream, OutputStream outputStream) throws IOException {
        DigestBuilder builder = new DigestBuilder();
        byte[] buffer = new byte[BUFFER_SIZE];
        int len = inputStream.read(buffer);
        while (len != -1) {
            builder.add(buffer, len);
            outputStream.write(buffer, 0, len);
            len = inputStream.read(buffer);
        }
        return builder.build();
    }
}
