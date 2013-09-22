package store.server.operation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import store.common.ContentInfo;
import store.common.Hash;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import static store.server.io.ObjectEncoder.encoder;

public class OperationManager {

    private final Path root;

    private OperationManager(Path root) {
        this.root = root;
    }

    public static OperationManager create(Path path) {
        try {
            Files.createDirectory(path);
            Files.createDirectory(path.resolve("pending"));
            Files.createDirectory(path.resolve("deleted"));
            return new OperationManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static OperationManager open(Path path) {
        if (!Files.isDirectory(path) ||
                !Files.isDirectory(path.resolve("pending")) ||
                !Files.isDirectory(path.resolve("deleted"))) {
            throw new InvalidStorePathException();
        }
        return new OperationManager(path);
    }

    public void beginPut(ContentInfo contentInfo) {
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
            throw new StoreRuntimeException(e);
        }
    }

    public void endPut(ContentInfo contentInfo) {
        remove("pending", contentInfo.getHash());
    }

    public void beginDelete(Hash hash) {
        try {
            Files.createFile(path("deleted", hash));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public void endDelete(Hash hash) {
        remove("deleted", hash);
    }

    private void remove(String dir, Hash hash) {
        try {
            Files.deleteIfExists(path(dir, hash));

        } catch (IOException e) {
            // TODO simplement logger l'erreur
        }
    }

    private Path path(String dir, Hash hash) {
        return root.resolve(dir).resolve(hash.encode());
    }
}
