package store.server.repository;

import static com.google.common.base.Joiner.on;
import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import store.common.Operation;
import store.common.RepositoryDef;
import store.common.RepositoryInfo;
import store.common.config.Config;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.server.exception.BadRequestException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.RepositoryClosedException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;
import store.server.storage.Command;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.Query;
import store.server.storage.StorageManager;
import store.server.task.TaskManager;

/**
 * Stores contents with their metadata and performs their asynchronous indexation transparently.
 */
public class Repository {

    private static final String STORAGE = "storage";
    private static final String CONTENT = "content";
    private static final String INDEX = "index";
    private static final String STATS = "stats";
    private static final String CUR_SEQS = "curSeqs";
    private static final Logger LOG = LoggerFactory.getLogger(Repository.class);
    private final RepositoryDef def;
    private final Signalable changesTracker;
    private final StorageManager storageManager;
    private final InfoManager infoManager;
    private final HistoryManager historyManager;
    private final StatsManager statsManager;
    private final ContentManager contentManager;
    private final Index index;
    private final IndexingAgent indexingAgent;
    private final StatsAgent statsAgent;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Repository(RepositoryDef def,
                       Config config,
                       TaskManager taskManager,
                       Signalable changesTracker,
                       ContentManager contentManager,
                       Index index) {
        this.def = def;
        this.changesTracker = changesTracker;
        storageManager = new StorageManager(def.getName(), def.getPath().resolve(STORAGE), config, taskManager);
        infoManager = new InfoManager(storageManager);
        historyManager = new HistoryManager(storageManager);
        statsManager = new StatsManager(storageManager);
        this.contentManager = contentManager;
        this.index = index;

        Database curSeqsDb = storageManager.openDatabase(CUR_SEQS);
        indexingAgent = new IndexingAgent(this, index, curSeqsDb, entry(INDEX));
        statsAgent = new StatsAgent(this, statsManager, curSeqsDb, entry(STATS));

        indexingAgent.start();
        statsAgent.start();
    }

    /**
     * Create a new repository.
     *
     * @param path Repository home. Expected not to exist.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param changesTracker An instance which tracks repository changes.
     * @return Created repository.
     */
    public static Repository create(Path path, Config config, TaskManager taskManager, Signalable changesTracker) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
            Files.createDirectory(path.resolve(STORAGE));

        } catch (IOException e) {
            throw new WriteException(e);
        }
        AttributesManager attributesManager = AttributesManager.create(path);
        String name = attributesManager.getName();
        Guid guid = attributesManager.getGuid();
        return new Repository(new RepositoryDef(name, guid, path),
                              config,
                              taskManager,
                              changesTracker,
                              ContentManager.create(path.resolve(CONTENT)),
                              Index.create(name, path.resolve(INDEX)));
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    /**
     * Open an existing repository.
     *
     * @param path Repository home.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param changesTracker An instance which tracks repository changes.
     * @return Opened repository.
     */
    public static Repository open(Path path, Config config, TaskManager taskManager, Signalable changesTracker) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        AttributesManager attributesManager = AttributesManager.open(path);
        String name = attributesManager.getName();
        Guid guid = attributesManager.getGuid();
        return new Repository(new RepositoryDef(name, guid, path),
                              config,
                              taskManager,
                              changesTracker,
                              ContentManager.open(path.resolve(CONTENT)),
                              Index.open(name, path.resolve(INDEX)));
    }

    /**
     * Provides the definition of this repository.
     *
     * @return A RepositoryDef instance.
     */
    public RepositoryDef getDef() {
        log("Returning repository def");
        return def;
    }

    /**
     * Provides various info about this repository.
     *
     * @return A RepositoryInfo instance.
     */
    public RepositoryInfo getInfo() {
        log("Returning repository info");
        if (closed.get()) {
            return new RepositoryInfo(def);
        }
        return new RepositoryInfo(def, statsManager.stats(), indexingAgent.info(), statsAgent.info());
    }

    /**
     * Close this repository, releasing underlying resources. Does nothing if it already closed. Any latter operation
     * will fail.
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        indexingAgent.stop();
        statsAgent.stop();
        index.close();
        storageManager.stop();
        contentManager.close();
    }

    /**
     * Add an content info revision. If associated content is not present, started transaction is suspended so that
     * caller may latter complete this operation by adding this content.
     *
     * @param contentInfo Content info revision.
     * @return Actual operation result.
     */
    public CommandResult addContentInfo(final ContentInfo contentInfo) {
        ensureOpen();
        log("Adding content info to {}, with head {}", contentInfo.getContent(), contentInfo.getParents());
        CommandResult result = storageManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = infoManager.put(contentInfo);
                handleCommandResult(result, contentInfo.getContent());
                return result;
            }
        });
        signalIf(!result.isNoOp() && result.getOperation() != Operation.CREATE);
        return result;
    }

    /**
     * Merge supplied content info revision tree with existing one, if any. If associated content is not present,
     * started transaction is suspended so that caller may latter complete this operation by creating this content.
     *
     * @param contentInfoTree Revision tree.
     * @return Actual operation result.
     */
    public CommandResult mergeContentInfoTree(final ContentInfoTree contentInfoTree) {
        ensureOpen();
        log("Merging content info tree of {}", contentInfoTree.getContent());
        CommandResult result = storageManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = infoManager.put(contentInfoTree);
                handleCommandResult(result, contentInfoTree.getContent());
                return result;
            }
        });
        signalIf(!result.isNoOp() && result.getOperation() != Operation.CREATE);
        return result;
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
        ensureOpen();
        log("Adding content {}", hash);
        CommandResult result = storageManager.inTransaction(transactionId, new Command() {
            @Override
            public CommandResult apply() {
                Optional<ContentInfoTree> treeOpt = infoManager.get(hash);
                if (!treeOpt.isPresent() || treeOpt.get().isDeleted()) {
                    // This is unexpected as we have a resumed transaction at this point.
                    throw new BadRequestException();
                }
                ContentInfoTree tree = treeOpt.get();
                contentManager.add(hash, tree.getLength(), source);
                historyManager.add(Operation.CREATE, hash, tree.getHead());
                return CommandResult.of(transactionId, Operation.CREATE, hash, tree.getHead());
            }
        });
        signalIf(true);
        return result;
    }

    /**
     * Delete a content.
     *
     * @param hash Hash of the content to delete.
     * @param head Hashes of expected head revisions of the info associated with the content.
     * @return Actual operation result.
     */
    public CommandResult deleteContent(final Hash hash, final SortedSet<Hash> head) {
        ensureOpen();
        log("Deleting content {}, with head {}", hash, head);
        CommandResult result = storageManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = infoManager.delete(hash, head);
                handleCommandResult(result, hash);
                return result;
            }
        });
        signalIf(!result.isNoOp());
        return result;
    }

    private void handleCommandResult(CommandResult result, Hash hash) {
        if (result.isNoOp()) {
            return;
        }
        Operation operation = result.getOperation();
        if (operation == Operation.CREATE) {
            storageManager.suspendCurrentTransaction();
            return;
        }
        if (operation == Operation.DELETE) {
            contentManager.delete(hash);
        }
        historyManager.add(operation, hash, result.getRevisions());
    }

    private void signalIf(boolean condition) {
        if (condition) {
            indexingAgent.signal();
            statsAgent.signal();
            changesTracker.signal(def.getGuid());
        }
    }

    /**
     * Provides content info tree associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info tree.
     */
    public ContentInfoTree getContentInfoTree(final Hash hash) {
        ensureOpen();
        log("Returning content info tree of {}", hash);
        return storageManager.inTransaction(new Query<ContentInfoTree>() {
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
     * Provides content info head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info head revisions.
     */
    public List<ContentInfo> getContentInfoHead(final Hash hash) {
        ensureOpen();
        log("Returning content info head of {}", hash);
        ContentInfoTree tree = getContentInfoTree(hash);
        return tree.get(tree.getHead());
    }

    /**
     * Provides requested content info revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @param revs Hash of revisions to return.
     * @return Corresponding revisions.
     */
    public List<ContentInfo> getContentInfoRevisions(Hash hash, Collection<Hash> revs) {
        ensureOpen();
        log("Returning content info revs of {} [{}]", hash, on(", ").join(revs));
        ContentInfoTree tree = getContentInfoTree(hash);
        return tree.get(revs);
    }

    /**
     * Provides an input stream on a content in this repository.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream getContent(final Hash hash) {
        ensureOpen();
        log("Returning content {}", hash);
        return storageManager.inTransaction(new Query<InputStream>() {
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
        ensureOpen();
        log("Returning content {} with head {}", hash, head);
        return storageManager.inTransaction(new Query<Optional<InputStream>>() {
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
     * Provides a paginated view of the history of this repository.
     *
     * @param chronological If true, returned list of events will sorted chronologically.
     * @param first Event sequence identifier to start with.
     * @param number Number of events to return.
     * @return A list of events.
     */
    public List<Event> history(final boolean chronological, final long first, final int number) {
        ensureOpen();
        log("Returning history{}, first {}, count {}", chronological ? "" : ", reverse", first, number);
        return storageManager.inTransaction(new Query<List<Event>>() {
            @Override
            public List<Event> apply() {
                return historyManager.history(chronological, first, number);
            }
        });
    }

    /**
     * Find index entries matching supplied query.
     *
     * @param query Search query.
     * @param first First result to return.
     * @param number Number of results to return.
     * @return A list of content hashes.
     */
    public List<IndexEntry> find(String query, int first, int number) {
        ensureOpen();
        return index.find(query, first, number);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new RepositoryClosedException();
        }
    }

    private void log(String format, Object... args) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        LOG.info(on("").join("[", def.getName(), "] ", format), args);
    }
}
