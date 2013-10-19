package store.server;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.IoUtil.copy;
import store.common.Properties.Common;
import store.common.Uid;
import store.server.exception.ConcurrentOperationException;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
import store.server.exception.UnknownHashException;
import store.server.exception.WriteException;
import store.server.index.IndexManager;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionManager;
import static store.server.transaction.TransactionManager.currentTransactionContext;

public class Store {

    private final Uid uid;
    private final TransactionManager transactionManager;
    private final IndexManager indexManager;
    private final HistoryManager historyManager;
    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Store(Uid uid,
                  TransactionManager transactionManager,
                  IndexManager indexManager,
                  HistoryManager historyManager,
                  InfoManager infoManager,
                  ContentManager contentManager) {
        this.uid = uid;
        this.transactionManager = transactionManager;
        this.indexManager = indexManager;
        this.historyManager = historyManager;
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    public static Store create(final Path path) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidStorePathException();
            }
        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
        final Uid uid = Uid.random();
        final TransactionManager txManager = TransactionManager.create(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Store>() {
            @Override
            public Store apply() {
                writeUid(path.resolve("uid"), uid);
                return new Store(uid,
                                 txManager,
                                 IndexManager.create(path.resolve("index")),
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

    public static Store open(final Path path) {
        final TransactionManager txManager = TransactionManager.open(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Store>() {
            @Override
            public Store apply() {
                return new Store(readUid(path.resolve("uid")),
                                 txManager,
                                 IndexManager.open(path.resolve("index")),
                                 HistoryManager.open(path.resolve("history")),
                                 InfoManager.open(path.resolve("info")),
                                 ContentManager.open(path.resolve("content")));
            }
        });
    }

    private static void writeUid(Path path, Uid uid) {
        try (OutputStream outputStream = Files.newOutputStream(path);
                Writer writer = new OutputStreamWriter(outputStream, Charsets.UTF_8);
                BufferedWriter bufferedWriter = new BufferedWriter(writer)) {

            bufferedWriter.write(uid.toString());

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    private static Uid readUid(Path path) {
        try (InputStream inputStream = Files.newInputStream(path);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                BufferedReader bufferedReader = new BufferedReader(reader)) {

            return new Uid(bufferedReader.readLine());

        } catch (IOException e) {
            throw new StoreRuntimeException(e);
        }
    }

    public Uid getUid() {
        return uid;
    }

    public void put(final ContentInfo contentInfo, final InputStream source) {
        final Hash hash = contentInfo.getHash();
        transactionManager.inTransaction(hash, new Command() {
            @Override
            public void apply() {
                ContentInfo info = contentInfo.with(Common.CAPTURE_DATE.key(),
                                                    currentTransactionContext().timestamp());
                try {
                    contentManager.put(contentInfo, source);
                    infoManager.put(contentInfo);
                    try (InputStream inputstream = contentManager.get(hash)) {
                        indexManager.index(info, inputstream);
                    }
                    historyManager.put(hash);

                } catch (IOException e) {
                    throw new WriteException(e);
                }
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

    public List<ContentInfo> find(final String query) {
        List<Hash> hashes = transactionManager.inTransaction(new Query<List<Hash>>() {
            @Override
            public List<Hash> apply() {
                return indexManager.find(query);
            }
        });
        List<ContentInfo> results = new ArrayList<>(hashes.size());
        for (Hash hash : hashes) {
            try {
                results.add(info(hash));

            } catch (UnknownHashException | ConcurrentOperationException e) {
            }
        }
        return results;
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
