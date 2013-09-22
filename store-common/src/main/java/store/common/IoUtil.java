package store.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import store.common.Digest.DigestBuilder;

public final class IoUtil {

    private static final int BUFFER_SIZE = 8192;

    private IoUtil() {
    }

    public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int len = inputStream.read(buffer);
        while (len != -1) {
            outputStream.write(buffer, 0, len);
            len = inputStream.read(buffer);
        }
    }

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
