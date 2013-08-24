package store;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.hash.Hash;
import store.info.ContentInfo;
import store.info.InfoManager;
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
                throw new IllegalArgumentException(path.toString());
            }
            return new Store(OperationManager.create(path.resolve("operations")),
                             InfoManager.create(path.resolve("info")),
                             ContentManager.create(path.resolve("content")));

        } catch (IOException e) {
            throw new RuntimeException(e);
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
        if (!operationManager.begin(contentInfo)) {
            throw new IllegalStateException("Operation concurrente");
        }
        try {
            contentManager.put(contentInfo, source);

        } catch (IOException e) {
            operationManager.abort(contentInfo);
            throw new RuntimeException(e);
        }
        operationManager.complete(contentInfo);
        infoManager.put(contentInfo);
        operationManager.clear(contentInfo);
    }

    public ContentReader get(Hash hash) {
        return new ContentReader(infoManager.get(hash), contentManager.get(hash));
    }

    public void delete(Hash hash) {
        if (!operationManager.delete(hash)) {
            throw new IllegalStateException("Operation concurrente");
        }
    }
}
