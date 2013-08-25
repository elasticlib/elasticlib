package store;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ConcurrentModificationException;
import store.exception.ContentAlreadyStoredException;
import store.exception.InvalidStorePathException;
import store.exception.StoreException;
import store.exception.StoreRuntimeException;
import store.exception.UnknownHashException;
import store.hash.Hash;
import store.info.ContentInfo;
import store.info.InfoManager;
import static store.operation.LockState.*;
import store.operation.OperationManager;

public class Store {

    private final OperationManager operationManager;
    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Store(OperationManager operationManager, InfoManager infoManager, ContentManager contentManager) {
        this.operationManager = operationManager;
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    public static Store create(Path path) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
            if (!isEmptyDir(path)) {
                throw new InvalidStorePathException();
            }
            return new Store(OperationManager.create(path.resolve("operations")),
                             InfoManager.create(path.resolve("info")),
                             ContentManager.create(path.resolve("content")));

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator()
                    .hasNext();
        }
    }

    public static Store open(Path path) {
        return new Store(OperationManager.open(path.resolve("operations")),
                         InfoManager.open(path.resolve("info")),
                         ContentManager.open(path.resolve("content")));
    }

    public void put(ContentInfo contentInfo, InputStream source) {
        if (operationManager.beginPut(contentInfo) == DENIED) {
            throw new ConcurrentModificationException();
        }
        try {
            if (infoManager.contains(contentInfo.getHash())) {
                throw new ContentAlreadyStoredException();
            }
            contentManager.put(contentInfo, source);

        } catch (StoreException e) {
            operationManager.abortPut(contentInfo);
            throw e;
        }
        operationManager.completePut(contentInfo);
        infoManager.put(contentInfo);
        operationManager.endPut(contentInfo);
    }

    public ContentReader get(Hash hash) {
        if (operationManager.beginGet(hash) == GRANTED) {
            Optional<ContentInfo> info = infoManager.get(hash);
            if (info.isPresent()) {
                return new ContentReader(this, info.get(), contentManager.get(hash));
            }
        }
        throw new UnknownHashException();
    }

    void close(Hash hash) {
        if (operationManager.endGet(hash) == ERASABLE) {
            erase(hash);
        }
    }

    public void delete(Hash hash) {
        switch (operationManager.beginDelete(hash)) {
            case DENIED:
                throw new ConcurrentModificationException();

            case GRANTED:
                return;

            case ERASABLE:
                erase(hash);
        }
    }

    private void erase(Hash hash) {
        infoManager.delete(hash);
        contentManager.delete(hash);
        operationManager.endDelete(hash);
    }
}
