package store.server.operation;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import store.server.exception.StoreRuntimeException;
import static store.server.io.ObjectEncoder.encoder;

final class Segment {

    private final Path path;

    public Segment(Path path) {
        this.path = path;
    }

    public void add(Step step) {
        byte[] bytes = encoder()
                .put("uid", step.getUid().value())
                .put("hash", step.getHash().value())
                .put("timestamp", step.getTimestamp())
                .put("OpCode", step.getOpCode().value())
                .build();

        try (OutputStream outputStream = Files.newOutputStream(path,
                                                               StandardOpenOption.CREATE,
                                                               StandardOpenOption.APPEND)) {
            outputStream.write(bytes);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }
}
