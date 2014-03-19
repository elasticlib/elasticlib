package store.server.volume;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import store.common.Operation;
import store.server.CommandResult;
import store.server.RevSpec;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionManager;

/**
 * Stores contents with their metadata. Each volume also maintains an history log. All read/write operations on a volume
 * are transactionnal.
 */
public class Volume {

    private final TransactionManager transactionManager;
    private final HistoryManager historyManager;
    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Volume(TransactionManager transactionManager,
                   HistoryManager historyManager,
                   InfoManager infoManager,
                   ContentManager contentManager) {
        this.transactionManager = transactionManager;
        this.historyManager = historyManager;
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    /**
     * Creates a new volume at specified path.
     *
     * @param path File-system path. Expected to not exists.
     * @return Created volume.
     */
    public static Volume create(final Path path) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        final TransactionManager txManager = TransactionManager.create(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Volume>() {
            @Override
            public Volume apply() {
                return new Volume(txManager,
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

    /**
     * Opens an existing volume.
     *
     * @param path Path to transaction manager home.
     * @return Opened volume.
     */
    public static Volume open(final Path path) {
        final TransactionManager txManager = TransactionManager.open(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Volume>() {
            @Override
            public Volume apply() {
                return new Volume(txManager,
                                  HistoryManager.open(path.resolve("history")),
                                  InfoManager.open(path.resolve("info")),
                                  ContentManager.open(path.resolve("content")));
            }
        });
    }

    /**
     * Starts this volume. Does nothing if it is already started.
     */
    public void start() {
        transactionManager.start();
    }

    /**
     * Stops this volume. Does nothing if it is already stopped.
     */
    public void stop() {
        transactionManager.stop();
    }

    /**
     * Checks if this volume is started. Other operations will fail if this is not the case.
     *
     * @return true if this volume is started.
     */
    public boolean isStarted() {
        return transactionManager.isStarted();
    }

    /**
     * Put a new content into this volume, along with a single info revision.
     *
     * @param contentInfo Content info revision.
     * @param source Content.
     * @param revSpec Expectations on current volume state for this content.
     */
    public void put(final ContentInfo contentInfo, final InputStream source, final RevSpec revSpec) {
        transactionManager.inTransaction(new Command() {
            @Override
            public void apply() {
                CommandResult result = infoManager.put(contentInfo, revSpec);
                handleCommandResult(result, contentInfo.getHash(), contentInfo.getLength(), source);
            }
        });
    }

    /**
     * Put a new content into this volume, along with a related revision tree.
     *
     * @param contentInfoTree Revision tree.
     * @param source Content.
     * @param revSpec Expectations on current volume state for this content.
     */
    public void put(final ContentInfoTree contentInfoTree, final InputStream source, final RevSpec revSpec) {
        transactionManager.inTransaction(new Command() {
            @Override
            public void apply() {
                CommandResult result = infoManager.put(contentInfoTree, revSpec);
                handleCommandResult(result, contentInfoTree.getHash(), contentInfoTree.getLength(), source);
            }
        });
    }

    private void handleCommandResult(CommandResult result, Hash hash, long length, InputStream source) {
        if (result.isNoOp()) {
            return;
        }
        Operation operation = result.getOperation();
        if (operation == Operation.CREATE || operation == Operation.RESTORE) {
            contentManager.add(hash, length, source);
        }
        if (operation == Operation.DELETE) {
            contentManager.delete(hash);
        }
        historyManager.add(hash, operation, result.getHead());
    }

    /**
     * Put an info revision into this volume.
     *
     * @param contentInfo Content info revision.
     * @param revSpec Expectations on current volume state for this content.
     */
    public void put(final ContentInfo contentInfo, final RevSpec revSpec) {
        transactionManager.inTransaction(new Command() {
            @Override
            public void apply() {
                CommandResult result = infoManager.put(contentInfo, revSpec);
                handleCommandResult(result, contentInfo.getHash());
            }
        });
    }

    /**
     * Put a revision tree into this volume.
     *
     * @param contentInfoTree Revision tree.
     * @param revSpec Expectations on current volume state for this content.
     */
    public void put(final ContentInfoTree contentInfoTree, final RevSpec revSpec) {
        transactionManager.inTransaction(new Command() {
            @Override
            public void apply() {
                CommandResult result = infoManager.put(contentInfoTree, revSpec);
                handleCommandResult(result, contentInfoTree.getHash());
            }
        });
    }

    /**
     * Delete a content from this volume.
     *
     * @param hash Hash of the content to delete.
     * @param revSpec Expectations on current volume state for this content.
     */
    public void delete(final Hash hash, final RevSpec revSpec) {
        transactionManager.inTransaction(new Command() {
            @Override
            public void apply() {
                CommandResult result = infoManager.delete(hash, revSpec);
                handleCommandResult(result, hash);
            }
        });
    }

    private void handleCommandResult(CommandResult result, Hash hash) {
        if (result.isNoOp()) {
            return;
        }
        Operation operation = result.getOperation();
        if (operation == Operation.CREATE || operation == Operation.RESTORE) {
            throw new UnknownContentException();
        }
        if (operation == Operation.DELETE) {
            contentManager.delete(hash);
        }
        historyManager.add(hash, operation, result.getHead());
    }

    /**
     * Provides info about a content in this volume.
     *
     * @param hash Hash of the content.
     * @return Corresponding info.
     */
    public ContentInfo info(final Hash hash) {
        return transactionManager.inTransaction(new Query<ContentInfo>() {
            @Override
            public ContentInfo apply() {
                Optional<ContentInfo> info = infoManager.get(hash);
                if (!info.isPresent()) {
                    throw new UnknownContentException();
                }
                return info.get();
            }
        });
    }

    /**
     * Provides an input stream on a content in this volume.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream get(final Hash hash) {
        return transactionManager.inTransaction(new Query<InputStream>() {
            @Override
            public InputStream apply() {
                return contentManager.get(hash);
            }
        });
    }

    /**
     * Provides a paginated view of the history of this volume.
     *
     * @param chronological If true, returned list of events will sorted chronologically.
     * @param first Event sequence identifier to start with.
     * @param number Number of events to return.
     * @return A list of events.
     */
    public List<Event> history(final boolean chronological, final long first, final int number) {
        return transactionManager.inTransaction(new Query<List<Event>>() {
            @Override
            public List<Event> apply() {
                return historyManager.history(chronological, first, number);
            }
        });
    }
}
