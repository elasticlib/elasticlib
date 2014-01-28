package store.client;

import java.io.IOException;
import java.io.InputStream;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.size;
import java.nio.file.Path;
import store.common.Digest;
import static store.common.IoUtil.copyAndDigest;
import static store.common.SinkOutputStream.sink;

public final class DigestUtil {

    private DigestUtil() {
    }

    public static Digest digest(Path filepath) {
        try (InputStream inputStream = new LoggingInputStream("Computing content digest",
                                                              newInputStream(filepath),
                                                              size(filepath))) {
            return copyAndDigest(inputStream, sink());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
