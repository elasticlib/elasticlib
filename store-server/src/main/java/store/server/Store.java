package store.server;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import store.common.Config;
import store.common.hash.Hash;
import store.common.info.ContentInfo;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import static store.server.operation.LockState.*;
import store.server.operation.OperationManager;

public class Store {

    private final OperationManager operationManager;
    private final List<Volume> volumes;

    private Store(OperationManager operationManager, List<Volume> volumes) {
        this.operationManager = operationManager;
        this.volumes = volumes;
    }

    public static Store create(Config config) {
        try {
            Path root = config.getRoot();
            Files.createDirectories(root);
            if (!isEmptyDir(root)) {
                throw new InvalidStorePathException();
            }
            List<Volume> volumes = new ArrayList<>();
            for (Path path : config.getVolumePaths()) {
                volumes.add(Volume.create(path));
            }
            return new Store(OperationManager.create(root.resolve("operations")), volumes);

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

    public static Store open(Config config) {
        Path root = config.getRoot();
        List<Volume> volumes = new ArrayList<>();
        for (Path path : config.getVolumePaths()) {
            volumes.add(Volume.open(path));
        }

        // TODO faire une reprise des operations !

        return new Store(OperationManager.open(root.resolve("operations")), volumes);
    }

    public void put(ContentInfo contentInfo, InputStream source) {
        if (operationManager.beginPut(contentInfo) == DENIED) {
            throw new ConcurrentModificationException();
        }
        try {
            Iterator<Volume> it = volumes.iterator();
            Volume first = it.next();
            first.put(contentInfo, source);
            while (it.hasNext()) {
                Volume next = it.next();
                try (ContentReader reader = reader(first, contentInfo.getHash())) {
                    next.put(contentInfo, reader.inputStream());
                }
            }
        } finally {
            operationManager.endPut(contentInfo);
        }
    }

    private ContentReader reader(Volume volume, Hash hash) {
        return volume.get(hash)
                .get()
                .reader(this);
    }

    public ContentInfo info(Hash hash) {
        if (operationManager.beginGet(hash) == GRANTED) {
            try {
                Optional<ContentInfo> info = volumes.get(0)
                        .info(hash);
                if (info.isPresent()) {
                    return info.get();
                }
            } finally {
                close(hash);
            }
        }
        throw new UnknownHashException();
    }

    public ContentReader get(Hash hash) {
        if (operationManager.beginGet(hash) == GRANTED) {
            Optional<Content> content = volumes.get(0)
                    .get(hash);
            if (content.isPresent()) {
                return content.get()
                        .reader(this);
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
                if (!volumes.get(0)
                        .contains(hash)) {
                    operationManager.endDelete(hash);
                    throw new UnknownHashException();
                }
                return;

            case ERASABLE:
                if (!volumes.get(0)
                        .contains(hash)) {
                    operationManager.endDelete(hash);
                    throw new UnknownHashException();
                }
                erase(hash);
        }
    }

    private void erase(Hash hash) {
        for (Volume volume : volumes) {
            volume.erase(hash);
        }
        operationManager.endDelete(hash);
    }
}
