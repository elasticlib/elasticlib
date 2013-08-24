package store;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import store.exception.InvalidStorePathException;
import store.exception.StoreException;
import store.exception.StoreRuntimeException;
import store.exception.UnknownHashException;
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
        operationManager.begin(contentInfo);
        try {
            contentManager.put(contentInfo, source);

        } catch (StoreException e) {
            operationManager.abort(contentInfo);
            throw e;
        }
        operationManager.complete(contentInfo);
        infoManager.put(contentInfo);
        operationManager.clear(contentInfo);
    }

    public ContentReader get(Hash hash) {
        // TODO Ajouter une operation read à operationManager (pas possible de read si deleted, pas de persistance)
        // store.delete() doit supprimer le contenu immédiatement si possible
        // sinon il le marque comme supprimable (peut se faire via des locks en mode read-write)
        // La fermeture du dernier contentReader ouvert lance la suppression
        Optional<ContentInfo> info = infoManager.get(hash);
        if (!info.isPresent()) {
            throw new UnknownHashException();
        }
        return new ContentReader(info.get(), contentManager.get(hash));
    }

    public void delete(Hash hash) {
        operationManager.delete(hash);
        infoManager.delete(hash);
        contentManager.delete(hash);
        operationManager.clear(hash);
    }
}
