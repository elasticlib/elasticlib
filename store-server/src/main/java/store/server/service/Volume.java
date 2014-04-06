package store.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.SortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import store.common.Operation;
import store.server.exception.BadRequestException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;
import store.server.transaction.Command;
import store.server.transaction.Query;
import store.server.transaction.TransactionContext;
import store.server.transaction.TransactionManager;

/**
 * Stores contents with their metadata. Each volume also maintains an history log. All read/write operations on a volume
 * are transactionnal.
 */
class Volume {

    private static final Logger LOG = LoggerFactory.getLogger(Volume.class);
    private final String name;
    private final TransactionManager transactionManager;
    private final HistoryManager historyManager;
    private final InfoManager infoManager;
    private final ContentManager contentManager;

    private Volume(String name,
                   TransactionManager transactionManager,
                   HistoryManager historyManager,
                   InfoManager infoManager,
                   ContentManager contentManager) {
        this.name = name;
        this.transactionManager = transactionManager;
        this.historyManager = historyManager;
        this.infoManager = infoManager;
        this.contentManager = contentManager;
    }

    /**
     * Creates a new volume at specified path.
     *
     * @param name Volume name.
     * @param path Volume home.
     * @return Created volume.
     */
    public static Volume create(final String name, final Path path) {
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
                return new Volume(name,
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

    /**
     * Opens an existing volume.
     *
     * @param name Volume name.
     * @param path Volume home.
     * @return Opened volume.
     */
    public static Volume open(final String name, final Path path) {
        final TransactionManager txManager = TransactionManager.open(path.resolve("transactions"));
        return txManager.inTransaction(new Query<Volume>() {
            @Override
            public Volume apply() {
                return new Volume(name,
                                  txManager,
                                  HistoryManager.open(path.resolve("history")),
                                  InfoManager.open(path.resolve("info")),
                                  ContentManager.open(path.resolve("content")));
            }
        });
    }

    /**
     * @return This volume name.
     */
    public String getName() {
        return name;
    }

    /**
     * Starts this volume. Does nothing if it is already started.
     */
    public void start() {
        LOG.info("[{}] Starting", name);
        transactionManager.start();
    }

    /**
     * Stops this volume. Does nothing if it is already stopped.
     */
    public void stop() {
        LOG.info("[{}] Stopping", name);
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
     * Add an info revision. If associated content is not present, started transaction is suspended so that caller may
     * latter complete this operation by adding this content.
     *
     * @param contentInfo Content info revision.
     * @return Actual operation result.
     */
    public CommandResult addInfo(final ContentInfo contentInfo) {
        LOG.info("[{}] Adding info for {}, with head {}", name, contentInfo.getContent(), contentInfo.getParents());
        return transactionManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = infoManager.put(contentInfo);
                handleCommandResult(result, contentInfo.getContent());
                return result;
            }
        });
    }

    /**
     * Merge a revision tree with existing one, if any. If associated content is not present, started transaction is
     * suspended so that caller may latter complete this operation by creating this content.
     *
     * @param contentInfoTree Revision tree.
     * @return Actual operation result.
     */
    public CommandResult mergeTree(final ContentInfoTree contentInfoTree) {
        LOG.info("[{}] Merging tree for {}", name, contentInfoTree.getContent());
        return transactionManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = infoManager.put(contentInfoTree);
                handleCommandResult(result, contentInfoTree.getContent());
                return result;
            }
        });
    }

    /**
     * Resume a previously suspended transaction and add content from supplied input-stream.
     *
     * @param transactionId Identifier of the transaction to resume.
     * @param hash Content hash. Used to retrieve its associated info.
     * @param source Content.
     * @return Operation result.
     */
    public CommandResult addContent(final long transactionId, final Hash hash, final InputStream source) {
        LOG.info("[{}] Adding content {}", name, hash);
        return transactionManager.inTransaction(transactionId, new Command() {
            @Override
            public CommandResult apply() {
                Optional<ContentInfoTree> treeOpt = infoManager.get(hash);
                if (!treeOpt.isPresent() || treeOpt.get().isDeleted()) {
                    // This is unexpected as we have a resumed transaction at this point.
                    throw new BadRequestException();
                }
                ContentInfoTree tree = treeOpt.get();
                contentManager.add(hash, tree.getLength(), source);
                historyManager.add(hash, Operation.CREATE, tree.getHead());
                return CommandResult.of(transactionId, Operation.CREATE, tree.getHead());
            }
        });
    }

    /**
     * Delete a content.
     *
     * @param hash Hash of the content to delete.
     * @param head Hashes of expected head revisions of the info associated with the content.
     * @return Actual operation result.
     */
    public CommandResult deleteContent(final Hash hash, final SortedSet<Hash> head) {
        LOG.info("[{}] Deleting content {}, with head {}", name, hash, head);
        return transactionManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = infoManager.delete(hash, head);
                handleCommandResult(result, hash);
                return result;
            }
        });
    }

    private void handleCommandResult(CommandResult result, Hash hash) {
        if (result.isNoOp()) {
            return;
        }
        Operation operation = result.getOperation();
        if (operation == Operation.CREATE) {
            TransactionContext.current().suspend();
            return;
        }
        if (operation == Operation.DELETE) {
            contentManager.delete(hash);
        }
        historyManager.add(hash, operation, result.getHead());
    }

    /**
     * Provides info tree associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info tree.
     */
    public ContentInfoTree getInfoTree(final Hash hash) {
        LOG.info("[{}] Returning info tree {}", name, hash);
        return transactionManager.inTransaction(new Query<ContentInfoTree>() {
            @Override
            public ContentInfoTree apply() {
                Optional<ContentInfoTree> tree = infoManager.get(hash);
                if (!tree.isPresent()) {
                    throw new UnknownContentException();
                }
                return tree.get();
            }
        });
    }

    /**
     * Provides info head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info head revisions.
     */
    public List<ContentInfo> getInfoHead(final Hash hash) {
        LOG.info("[{}] Returning info head {}", name, hash);
        return transactionManager.inTransaction(new Query<List<ContentInfo>>() {
            @Override
            public List<ContentInfo> apply() {
                Optional<ContentInfoTree> treeOpt = infoManager.get(hash);
                if (!treeOpt.isPresent()) {
                    return emptyList();
                }
                ContentInfoTree tree = treeOpt.get();
                return tree.get(tree.getHead());
            }
        });
    }

    /**
     * Provides requested info revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @param revs Hash of revisions to return.
     * @return Corresponding revisions.
     */
    public List<ContentInfo> getInfoRevisions(Hash hash, Collection<Hash> revs) {
        LOG.info("[{}] Returning info revs {} [{}]", name, hash, Joiner.on(", ").join(revs));
        ContentInfoTree tree = getInfoTree(hash);
        return tree.get(revs);
    }

    /**
     * Provides an input stream on a content in this volume.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream getContent(final Hash hash) {
        LOG.info("[{}] Returning content {}", name, hash);
        return transactionManager.inTransaction(new Query<InputStream>() {
            @Override
            public InputStream apply() {
                return contentManager.get(hash);
            }
        });
    }

    /**
     * Provides an input stream on a content if its info head matches supplied one.
     *
     * @param hash Hash of the content.
     * @param head Hashes of expected head revisions of the info associated with the content.
     * @return An input stream on this content, or nothing if supplied head has been superseded.
     */
    public Optional<InputStream> getContent(final Hash hash, final SortedSet<Hash> head) {
        LOG.info("[{}] Returning content {} with head {}", name, hash, head);
        return transactionManager.inTransaction(new Query<Optional<InputStream>>() {
            @Override
            public Optional<InputStream> apply() {
                Optional<ContentInfoTree> tree = infoManager.get(hash);
                if (!tree.isPresent()) {
                    throw new UnknownContentException();
                }
                if (!tree.get().getHead().equals(head)) {
                    return Optional.absent();
                }
                return Optional.of(contentManager.get(hash));
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
        LOG.info("[{}] Returning history{}, first {}, count {}", name, chronological ? "" : ", reverse", first, number);
        return transactionManager.inTransaction(new Query<List<Event>>() {
            @Override
            public List<Event> apply() {
                return historyManager.history(chronological, first, number);
            }
        });
    }
}
