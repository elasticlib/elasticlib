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
import store.common.config.Config;
import store.common.exception.BadRequestException;
import store.common.exception.ContentAlreadyPresentException;
import store.common.exception.IOFailureException;
import store.common.exception.InvalidRepositoryPathException;
import store.common.exception.RepositoryClosedException;
import store.common.exception.UnknownContentException;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentInfoTree;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.Operation;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import store.common.model.StagingInfo;
import store.server.manager.message.MessageManager;
import store.server.manager.message.NewRepositoryEvent;
import store.server.manager.storage.Command;
import static store.server.manager.storage.DatabaseEntries.entry;
import store.server.manager.storage.Query;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.TaskManager;

/**
 * Stores contents with their metadata and performs their asynchronous indexation transparently.
 */
public class Repository {

    private static final String STORAGE = "storage";
    private static final String INDEX = "index";
    private static final String STATS = "stats";
    private static final String CUR_SEQS = "curSeqs";
    private static final Logger LOG = LoggerFactory.getLogger(Repository.class);
    private final RepositoryDef def;
    private final StorageManager storageManager;
    private final MessageManager messageManager;
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
                       MessageManager messageManager,
                       ContentManager contentManager,
                       Index index) {
        this.def = def;
        storageManager = new StorageManager(def.getName(), def.getPath().resolve(STORAGE), config, taskManager);
        this.messageManager = messageManager;
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
     * Creates a new repository.
     *
     * @param path Repository home. Expected not to exist.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param messageManager Messaging infrastructure manager.
     * @return Created repository.
     */
    public static Repository create(Path path, Config config, TaskManager taskManager, MessageManager messageManager) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
            Files.createDirectory(path.resolve(STORAGE));

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
        AttributesManager attributesManager = AttributesManager.create(path);
        String name = attributesManager.getName();
        Guid guid = attributesManager.getGuid();
        return new Repository(new RepositoryDef(name, guid, path),
                              config,
                              taskManager,
                              messageManager,
                              ContentManager.create(name, path, config, taskManager),
                              Index.create(name, path));
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    /**
     * Opens an existing repository.
     *
     * @param path Repository home.
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param messageManager Messaging infrastructure manager.
     * @return Opened repository.
     */
    public static Repository open(Path path, Config config, TaskManager taskManager, MessageManager messageManager) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        AttributesManager attributesManager = AttributesManager.open(path);
        String name = attributesManager.getName();
        Guid guid = attributesManager.getGuid();
        return new Repository(new RepositoryDef(name, guid, path),
                              config,
                              taskManager,
                              messageManager,
                              ContentManager.open(name, path, config, taskManager),
                              Index.open(name, path));
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
     * Prepares to add a new content in this repository.
     *
     * @param hash Hash of the content to be added latter.
     * @return Info about the staging session created.
     */
    public StagingInfo stage(final Hash hash) {
        ensureOpen();
        log("Staging content {}", hash);
        return storageManager.inTransaction(new Query<StagingInfo>() {
            @Override
            public StagingInfo apply() {
                Optional<ContentInfoTree> treeOpt = infoManager.get(hash);
                if (treeOpt.isPresent() && !treeOpt.get().isDeleted()) {
                    throw new ContentAlreadyPresentException();
                }

                // TODO this is a stub !
                throw new UnsupportedOperationException();
            }
        });
    }

    /**
     * Writes bytes to a staged content.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     * @param source Bytes to write.
     * @param position Position in staged content at which write should begin.
     * @return Updated info of the staging session.
     */
    public StagingInfo write(Hash hash, Guid sessionId, InputStream source, long position) {
        ensureOpen();
        log("Writing to staged content {}", hash);

        // TODO this is a stub !
        throw new UnsupportedOperationException();
    }

    /**
     * Adds an content info revision. If associated content is not present, started transaction is suspended so that
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
     * Merges supplied content info revision tree with existing one, if any. If associated content is not present,
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
     * Resumes a previously suspended transaction and add content from supplied input-stream.
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
                SortedSet<Hash> head = treeOpt.get().getHead();
                contentManager.add(hash, source);
                historyManager.add(Operation.CREATE, hash, head);
                return CommandResult.of(transactionId, Operation.CREATE, hash, head);
            }
        });
        signalIf(true);
        return result;
    }

    /**
     * Deletes a content.
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
            messageManager.post(new NewRepositoryEvent(def.getGuid()));
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
        return loadContentInfoTree(hash);
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
        ContentInfoTree tree = loadContentInfoTree(hash);
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
        return loadContentInfoTree(hash).get(revs);
    }

    private ContentInfoTree loadContentInfoTree(final Hash hash) {
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
     * Provides an input stream on a content in this repository.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream getContent(final Hash hash) {
        ensureOpen();
        log("Returning content {}", hash);
        return contentManager.get(hash);
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
     * Finds index entries matching supplied query.
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
