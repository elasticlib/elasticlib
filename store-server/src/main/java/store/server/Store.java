package store.server;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import store.common.Config;
import store.common.ContentInfo;
import store.common.Hash;
import static store.common.IoUtil.copy;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import store.server.exception.WriteException;
import store.server.lock.LockManager;
import store.server.operation.OperationManager;

public class Store {

    private final LockManager lockManager = new LockManager();
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
            return !stream.iterator().hasNext();
        }
    }

    public static Store open(Config config) {
        Path root = config.getRoot();
        List<Volume> volumes = new ArrayList<>();
        for (Path path : config.getVolumePaths()) {
            volumes.add(Volume.open(path));
        }
        return new Store(OperationManager.open(root.resolve("operations")), volumes);
    }

    public void put(ContentInfo contentInfo, InputStream source) {
        Hash hash = contentInfo.getHash();
        if (!lockManager.writeLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            Iterator<Volume> it = volumes.iterator();
            Volume first = it.next();
            operationManager.put(first.getUid(), hash);
            first.put(contentInfo, source);
            while (it.hasNext()) {
                Volume next = it.next();
                try (InputStream inputstream = first.get(hash)) {
                    operationManager.put(next.getUid(), hash);
                    next.put(contentInfo, inputstream);

                } catch (IOException e) {
                    throw new StoreRuntimeException(e);
                }
            }
        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    public void delete(Hash hash) {
        if (!lockManager.writeLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            for (Volume volume : volumes) {
                Uid uid = volume.getUid();
                operationManager.delete(uid, hash);
                volume.delete(hash);
            }
        } finally {
            lockManager.writeUnlock(hash);
        }
    }

    public boolean contains(Hash hash) {
        if (!lockManager.readLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            return volumes.get(0).contains(hash);

        } finally {
            lockManager.readUnlock(hash);
        }
    }

    public ContentInfo info(Hash hash) {
        if (!lockManager.readLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            Optional<ContentInfo> info = volumes.get(0).info(hash);
            if (!info.isPresent()) {
                throw new UnknownHashException();
            }
            return info.get();

        } finally {
            lockManager.readUnlock(hash);
        }
    }

    public void get(Hash hash, OutputStream outputStream) {
        if (!lockManager.readLock(hash)) {
            throw new ConcurrentModificationException();
        }
        try {
            if (!volumes.get(0).contains(hash)) {
                throw new UnknownHashException();
            }
            try (InputStream inputStream = volumes.get(0).get(hash)) {
                copy(inputStream, outputStream);

            } catch (IOException e) {
                throw new WriteException(e);
            }
        } finally {
            lockManager.readUnlock(hash);
        }
    }
}
