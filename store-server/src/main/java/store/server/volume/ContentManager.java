package store.server.volume;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.common.Digest;
import store.common.Hash;
import static store.common.IoUtil.copyAndDigest;
import store.server.exception.IntegrityCheckingFailedException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.WriteException;
import store.server.lock.Table;
import store.server.transaction.Output;
import store.server.transaction.TransactionContext;
import static store.server.transaction.TransactionManager.currentTransactionContext;

class ContentManager {

    private static final int HEAVY_WRITE_THRESHOLD = 1024;
    private static final int KEY_LENGTH = 2;
    private final Path root;

    private ContentManager(Path root) {
        this.root = root;
    }

    public static ContentManager create(Path path) {
        try {
            Files.createDirectory(path);
            for (String key : Table.keySet(KEY_LENGTH)) {
                Files.createDirectory(path.resolve(key));
            }
            return new ContentManager(path);

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public static ContentManager open(Path path) {
        if (!Files.isDirectory(path)) {
            throw new InvalidStorePathException();
        }
        for (String key : Table.keySet(KEY_LENGTH)) {
            if (!Files.isDirectory(path.resolve(key))) {
                throw new InvalidStorePathException();
            }
        }
        return new ContentManager(path);
    }

    public void add(Hash hash, long length, InputStream source) {
        try (Output target = openOutput(hash, length)) {
            Digest digest = copyAndDigest(source, target);
            if (length != digest.getLength() || !hash.equals(digest.getHash())) {
                throw new IntegrityCheckingFailedException();
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    private Output openOutput(Hash hash, long length) {
        TransactionContext txContext = currentTransactionContext();
        Path path = path(hash);
        txContext.create(path);
        if (length > HEAVY_WRITE_THRESHOLD) {
            return txContext.openHeavyWriteOutput(path);
        }
        return txContext.openOutput(path);
    }

    public boolean contains(Hash hash) {
        return currentTransactionContext().exists(path(hash));
    }

    public InputStream get(Hash hash) {
        return currentTransactionContext().openInput(path(hash));
    }

    public void delete(Hash hash) {
        currentTransactionContext().delete(path(hash));
    }

    private Path path(Hash hash) {
        String encoded = hash.encode();
        return root.resolve(encoded.substring(0, KEY_LENGTH)).resolve(encoded);
    }
}
