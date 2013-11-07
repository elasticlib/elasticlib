package store.server.volume;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.IoUtil.copy;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import store.server.exception.WriteException;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionManager;

/**
 * A volume. Store contents with their metadata. Each volume also maintains an history log. All read/write operations on
 * a volume are transactionnal.
 */
public class Volume {

    private final Path path;
    private final TransactionManager transactionManager;
    private final HistoryManager historyManager;
    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Volume(Path path,
                   TransactionManager transactionManager,
                   HistoryManager historyManager,
                   InfoManager infoManager,
                   ContentManager contentManager) {
        this.path = path;
        this.transactionManager = transactionManager;
        this.historyManager = historyManager;
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    public static Volume create(final Path path) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidStorePathException();
            }
        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
        final TransactionManager txManager = TransactionManager.create(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Volume>() {
            @Override
            public Volume apply() {
                return new Volume(path,
                                  txManager,
                                  HistoryManager.create(path.resolve("history")),
                                  InfoManager.create(path.resolve("info")),
                                  ContentManager.create(path.resolve("content")));
            }
        });
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    public static Volume open(final Path path) {
        final TransactionManager txManager = TransactionManager.open(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Volume>() {
            @Override
            public Volume apply() {
                return new Volume(path,
                                  txManager,
                                  HistoryManager.open(path.resolve("history")),
                                  InfoManager.open(path.resolve("info")),
                                  ContentManager.open(path.resolve("content")));
            }
        });
    }

    public String getName() {
        return path.getFileName().toString();
    }

    public Path getPath() {
        return path;
    }

    public void start() {
        transactionManager.start();
    }

    public void stop() {
        transactionManager.stop();
    }

    public void put(final ContentInfo contentInfo, final InputStream source) {
        final Hash hash = contentInfo.getHash();
        transactionManager.inTransaction(hash, new Command() {
            @Override
            public void apply() {
                infoManager.put(contentInfo);
                contentManager.put(contentInfo, source);
                historyManager.put(hash);
            }
        });
    }

    public void delete(final Hash hash) {
        transactionManager.inTransaction(hash, new Command() {
            @Override
            public void apply() {
                infoManager.delete(hash);
                contentManager.delete(hash);
                historyManager.delete(hash);
            }
        });
    }

    public boolean contains(final Hash hash) {
        return transactionManager.inTransaction(hash, new Query<Boolean>() {
            @Override
            public Boolean apply() {
                return infoManager.contains(hash);
            }
        });
    }

    public ContentInfo info(final Hash hash) {
        return transactionManager.inTransaction(hash, new Query<ContentInfo>() {
            @Override
            public ContentInfo apply() {
                Optional<ContentInfo> info = infoManager.get(hash);
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
                if (!infoManager.contains(hash)) {
                    throw new UnknownHashException();
                }
                try (InputStream inputStream = contentManager.get(hash)) {
                    copy(inputStream, outputStream);

                } catch (IOException e) {
                    throw new WriteException(e);
                }
                return null;
            }
        });
    }

    public List<Event> history(final boolean chronological, final long first, final int number) {
        return transactionManager.inTransaction(new Query<List<Event>>() {
            @Override
            public List<Event> apply() {
                return historyManager.history(chronological, first, number);
            }
        });
    }
}
