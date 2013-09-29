package store.server;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import store.common.Config;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.IoUtil.copy;
import store.common.Uid;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import store.server.exception.WriteException;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionManager;
import store.server.volume.Volume;

public class Store {

    private final TransactionManager transactionManager;
    private final HistoryManager historyManager;
    private final List<Volume> volumes;

    private Store(TransactionManager transactionManager, HistoryManager historyManager, List<Volume> volumes) {
        this.transactionManager = transactionManager;
        this.historyManager = historyManager;
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
            return new Store(TransactionManager.create(root.resolve("transactions")),
                             HistoryManager.create(root.resolve("history")),
                             volumes);

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
        return new Store(TransactionManager.open(root.resolve("transactions")),
                         HistoryManager.open(root.resolve("history")),
                         volumes);
    }

    private Set<Uid> volumeUids() {
        Set<Uid> uids = new HashSet<>();
        for (Volume volume : volumes) {
            uids.add(volume.getUid());
        }
        return uids;
    }

    public void put(final ContentInfo contentInfo, final InputStream source) {
        final Hash hash = contentInfo.getHash();
        transactionManager.inTransaction(hash, new Command() {
            @Override
            public void apply() {
                Iterator<Volume> it = volumes.iterator();
                Volume first = it.next();
                first.put(contentInfo, source);
                while (it.hasNext()) {
                    Volume next = it.next();
                    try (InputStream inputstream = first.get(hash)) {
                        next.put(contentInfo, inputstream);

                    } catch (IOException e) {
                        throw new WriteException(e);
                    }
                }
                historyManager.put(hash, volumeUids());
            }
        });
    }

    public void delete(final Hash hash) {
        transactionManager.inTransaction(hash, new Command() {
            @Override
            public void apply() {
                for (Volume volume : volumes) {
                    volume.delete(hash);
                }
                historyManager.delete(hash, volumeUids());
            }
        });
    }

    public boolean contains(final Hash hash) {
        return transactionManager.inTransaction(hash, new Query<Boolean>() {
            @Override
            public Boolean apply() {
                return volumes.get(0).contains(hash);
            }
        });
    }

    public ContentInfo info(final Hash hash) {
        return transactionManager.inTransaction(hash, new Query<ContentInfo>() {
            @Override
            public ContentInfo apply() {
                Optional<ContentInfo> info = volumes.get(0).info(hash);
                if (!info.isPresent()) {
                    throw new UnknownHashException();
                }
                return info.get();
            }
        });
    }

    public void get(final Hash hash, final OutputStream outputStream) {
        transactionManager.inTransaction(hash, new Query<Void>() {
            @Override
            public Void apply() {
                if (!volumes.get(0).contains(hash)) {
                    throw new UnknownHashException();
                }
                try (InputStream inputStream = volumes.get(0).get(hash)) {
                    copy(inputStream, outputStream);

                } catch (IOException e) {
                    throw new WriteException(e);
                }
                return null;
            }
        });
    }

    public List<Event> history() {
        return transactionManager.inTransaction(new Query< List<Event>>() {
            @Override
            public List<Event> apply() {
                return historyManager.history();
            }
        });
    }
}
