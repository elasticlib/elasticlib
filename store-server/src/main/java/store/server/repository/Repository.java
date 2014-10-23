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
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.common.exception.ContentAlreadyPresentException;
import store.common.exception.IOFailureException;
import store.common.exception.InvalidRepositoryPathException;
import store.common.exception.RepositoryClosedException;
import store.common.exception.UnknownContentException;
import store.common.hash.Guid;
import store.common.hash.Hash;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentState;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.Operation;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import store.common.model.Revision;
import store.common.model.RevisionTree;
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
    private final RevisionManager revisionManager;
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
        revisionManager = new RevisionManager(storageManager);
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
    public StagingInfo stageContent(final Hash hash) {
        ensureOpen();
        log("Staging content {}", hash);
        return storageManager.inTransaction(new Query<StagingInfo>() {
            @Override
            public StagingInfo apply() {
                Optional<RevisionTree> treeOpt = revisionManager.get(hash);
                if (treeOpt.isPresent() && !treeOpt.get().isDeleted()) {
                    throw new ContentAlreadyPresentException();
                }
                return contentManager.stageContent(hash);
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
    public StagingInfo writeContent(Hash hash, Guid sessionId, InputStream source, long position) {
        ensureOpen();
        log("Writing to staged content {}", hash);
        return contentManager.writeContent(hash, sessionId, source, position);
    }

    /**
     * Terminates a content staging session. Actually, this only releases the session, but leaves staged content as it.
     * Does nothing if such a session does not exist or has expired.
     *
     * @param hash Hash of the staged content (when staging is completed).
     * @param sessionId Staging session identifier.
     */
    public void unstageContent(Hash hash, Guid sessionId) {
        ensureOpen();
        log("Ending staging staging session {}", sessionId);
        contentManager.unstageContent(hash, sessionId);
    }

    /**
     * Adds a revision. If associated content is not present, started transaction is suspended so that caller may latter
     * complete this operation by adding this content.
     *
     * @param revision revision.
     * @return Actual operation result.
     */
    public CommandResult addRevision(final Revision revision) {
        ensureOpen();
        log("Adding revision to {}, with head {}", revision.getContent(), revision.getParents());
        CommandResult result = storageManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = revisionManager.put(revision);
                handleCommandResult(result, revision.getContent());
                return result;
            }
        });
        propagate(result);
        return result;
    }

    /**
     * Merges supplied revision tree with existing one, if any. If associated content is not present, started
     * transaction is suspended so that caller may latter complete this operation by creating this content.
     *
     * @param tree Revision tree.
     * @return Actual operation result.
     */
    public CommandResult mergeTree(final RevisionTree tree) {
        ensureOpen();
        log("Merging revision tree of {}", tree.getContent());
        CommandResult result = storageManager.inTransaction(new Command() {
            @Override
            public CommandResult apply() {
                CommandResult result = revisionManager.put(tree);
                handleCommandResult(result, tree.getContent());
                return result;
            }
        });
        propagate(result);
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
                CommandResult result = revisionManager.delete(hash, head);
                handleCommandResult(result, hash);
                return result;
            }
        });
        propagate(result);
        return result;
    }

    private void handleCommandResult(CommandResult result, Hash hash) {
        if (result.isNoOp()) {
            return;
        }
        Operation operation = result.getOperation();
        historyManager.add(operation, hash, result.getRevisions());
        if (operation == Operation.CREATE) {
            contentManager.add(hash);
        }
        if (operation == Operation.DELETE) {
            contentManager.delete(hash);
        }
    }

    private void propagate(CommandResult result) {
        if (result.isNoOp()) {
            return;
        }
        indexingAgent.signal();
        statsAgent.signal();
        messageManager.post(new NewRepositoryEvent(def.getGuid()));
    }

    /**
     * Provides info about a given content.
     *
     * @param hash Hash of the content.
     * @return Corresponding content info.
     */
    public ContentInfo getContentInfo(final Hash hash) {
        ensureOpen();
        log("Returning content info of {}", hash);
        return storageManager.inTransaction(new Query<ContentInfo>() {
            @Override
            public ContentInfo apply() {
                List<Revision> head = head(hash);
                StagingInfo stagingInfo = contentManager.getStagingInfo(hash);
                return new ContentInfo(contentState(hash, head, stagingInfo),
                                       stagingInfo.getHash(),
                                       stagingInfo.getLength(),
                                       head);
            }
        });
    }

    private List<Revision> head(Hash hash) {
        Optional<RevisionTree> treeOpt = revisionManager.get(hash);
        if (treeOpt.isPresent()) {
            RevisionTree tree = treeOpt.get();
            return tree.get(tree.getHead());
        }
        return emptyList();
    }

    private static ContentState contentState(Hash hash, List<Revision> head, StagingInfo stagingInfo) {
        if (isPresent(head)) {
            return ContentState.PRESENT;
        }
        if (stagingInfo.getHash().equals(hash)) {
            return ContentState.STAGED;
        }
        if (stagingInfo.getSessionId() != null) {
            return ContentState.STAGING;
        }
        if (stagingInfo.getLength() == 0) {
            return ContentState.ABSENT;
        }
        return ContentState.PARTIAL;
    }

    private static boolean isPresent(List<Revision> head) {
        for (Revision rev : head) {
            if (!rev.isDeleted()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Provides revision tree associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding revision tree.
     */
    public RevisionTree getTree(Hash hash) {
        ensureOpen();
        log("Returning revision tree of {}", hash);
        return loadRevisionTree(hash);
    }

    /**
     * Provides head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding head revisions.
     */
    public List<Revision> getHead(Hash hash) {
        ensureOpen();
        log("Returning head revisions of {}", hash);
        RevisionTree tree = loadRevisionTree(hash);
        return tree.get(tree.getHead());
    }

    /**
     * Provides requested revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @param revs Hash of the revisions to return.
     * @return Corresponding revisions.
     */
    public List<Revision> getRevisions(Hash hash, Collection<Hash> revs) {
        ensureOpen();
        log("Returning revisions of {} [{}]", hash, on(", ").join(revs));
        return loadRevisionTree(hash).get(revs);
    }

    private RevisionTree loadRevisionTree(final Hash hash) {
        return storageManager.inTransaction(new Query<RevisionTree>() {
            @Override
            public RevisionTree apply() {
                Optional<RevisionTree> tree = revisionManager.get(hash);
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
     * @param offset The position of first byte to return, inclusive. Expected to be positive or zero.
     * @param length The maximum amount of bytes to returns. Expected to be positive or zero.
     * @return An input stream on this content.
     */
    public InputStream getContent(Hash hash, long offset, long length) {
        ensureOpen();
        log("Returning content {}, offset {}, length {}", hash, offset, length);
        return contentManager.get(hash, offset, length);
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
