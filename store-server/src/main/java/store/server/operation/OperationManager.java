package store.server.operation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import store.common.hash.Hash;
import store.common.info.ContentInfo;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import static store.server.io.ObjectEncoder.encoder;
import static store.server.operation.LockState.*;

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

    public LockState beginPut(ContentInfo contentInfo) {
        Hash hash = contentInfo.getHash();
        if (locks.putLock(hash) == DENIED) {
            return DENIED;
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
        return GRANTED;
    }

    public void endPut(ContentInfo contentInfo) {
        remove("pending", contentInfo.getHash());
        locks.putUnlock(contentInfo.getHash());
    }

    public LockState beginGet(Hash hash) {
        return locks.getLock(hash);
    }

    public LockState endGet(Hash hash) {
        return locks.getUnlock(hash);
    }

    public LockState beginDelete(Hash hash) {
        LockState state = locks.deleteLock(hash);
        if (state == DENIED) {
            return DENIED;
        }
        try {
            Files.createFile(path("deleted", hash));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
        return state;
    }

    public void endDelete(Hash hash) {
        remove("deleted", hash);
        locks.deleteUnlock(hash);
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
