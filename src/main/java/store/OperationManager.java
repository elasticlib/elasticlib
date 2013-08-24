package store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import store.hash.Hash;
import store.info.ContentInfo;
import static store.io.ObjectEncoder.encoder;

public class OperationManager {

    private final Path root;

    public OperationManager(Path root) {
        this.root = root;
    }

    public void begin(ContentInfo contentInfo) {
        Hash hash = contentInfo.getHash();
        Path path = path("pending", hash);
        byte[] bytes = encoder()
                .put("hash", hash.value())
                .put("length", contentInfo.getLength())
                .put("metadata", contentInfo.getMetadata())
                .build();
        try {
            Files.write(path, bytes, StandardOpenOption.CREATE_NEW);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void complete(ContentInfo contentInfo) {
        Hash hash = contentInfo.getHash();
        Path source = path("pending", hash);
        Path target = path("completed", hash);
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(Hash hash) {
        Path path = path("deleted", hash);
        try {
            Files.createFile(path);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path path(String dir, Hash hash) {
        return root.resolve(dir)
                .resolve(hash.encode());
    }
}
