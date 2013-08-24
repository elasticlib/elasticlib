package store.operation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import store.exception.ConcurrentOperationException;
import store.exception.InvalidStorePathException;
import store.exception.StoreRuntimeException;
import store.hash.Hash;
import store.info.ContentInfo;
import static store.io.ObjectEncoder.encoder;

public class OperationManager {

    private final Path root;
    private final Locks locks;

    private OperationManager(Path root) {
        this.root = root;
        locks = new Locks();
    }

    public static OperationManager create(Path path) {
        try {
            Files.createDirectory(path);
            Files.createDirectory(path.resolve("pending"));
            Files.createDirectory(path.resolve("completed"));
            Files.createDirectory(path.resolve("deleted"));
            return new OperationManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static OperationManager open(Path path) {
        if (!Files.isDirectory(path) ||
                !Files.isDirectory(path.resolve("pending")) ||
                !Files.isDirectory(path.resolve("completed")) ||
                !Files.isDirectory(path.resolve("deleted"))) {
            throw new InvalidStorePathException();
        }
        return new OperationManager(path);
    }

    public void begin(ContentInfo contentInfo) {
        Hash hash = contentInfo.getHash();
        if (!locks.lock(hash)) {
            throw new ConcurrentOperationException();
        }
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

    public void abort(ContentInfo contentInfo) {
        remove("pending", contentInfo.getHash());
    }

    public void complete(ContentInfo contentInfo) {
        Hash hash = contentInfo.getHash();
        Path source = path("pending", hash);
        Path target = path("completed", hash);
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public void clear(ContentInfo contentInfo) {
        remove("completed", contentInfo.getHash());
    }

    private void remove(String dir, Hash hash) {
        try {
            Files.deleteIfExists(path(dir, hash));

        } catch (IOException e) {
            // TODO simplement logger l'erreur
        } finally {
            locks.unlock(hash);
        }
    }

    public void delete(Hash hash) {
        if (!locks.lock(hash)) {
            throw new ConcurrentOperationException();
        }
        try {
            Files.createFile(path("deleted", hash));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public void clear(Hash hash) {
        remove("deleted", hash);
    }

    private Path path(String dir, Hash hash) {
        return root.resolve(dir)
                .resolve(hash.encode());
    }
}
