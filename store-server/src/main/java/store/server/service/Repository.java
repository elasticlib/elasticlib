package store.server.service;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import store.common.Operation;
import store.common.hash.Hash;
import store.server.exception.BadRequestException;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.UnknownContentException;
import store.server.exception.WriteException;
import store.server.storage.Command;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Stores contents with their metadata and performs their asynchronous indexation transparently.
 */
public class Repository {

    private static final String STORAGE = "storage";
    private static final String CONTENT = "content";
    private static final String INDEX = "index";
    private static final String INDEXATION_CURSOR = "indexationCursor";
    private static final Logger LOG = LoggerFactory.getLogger(Repository.class);
    private final String name;
    private final Path path;
    private final ReplicationService replicationService;
    private final StorageManager storageManager;
    private final InfoManager infoManager;
    private final HistoryManager historyManager;
    private final ContentManager contentManager;
    private final Index index;
    private final IndexingAgent agent;

    private Repository(String name,
                       Path path,
                       ScheduledExecutorService executor,
                       ReplicationService replicationService,
                       ContentManager contentManager,
                       Index index) {
        this.name = name;
        this.path = path;
        this.replicationService = replicationService;
        storageManager = new StorageManager(name, path.resolve(STORAGE), executor);
        infoManager = new InfoManager(storageManager);
        historyManager = new HistoryManager(storageManager);
        this.contentManager = contentManager;
        this.index = index;
        agent = new IndexingAgent(this, index, storageManager.openDatabase(INDEXATION_CURSOR));
        agent.start();
    }

    /**
     * Create a new repository.
     *
     * @param path Repository home. Expected not to exist.
     * @param executor Executor service.
     * @param replicationService Replication service.
     * @return Created repository.
     */
    static Repository create(Path path, ScheduledExecutorService executor, ReplicationService replicationService) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
            Files.createDirectory(path.resolve(STORAGE));

        } catch (IOException e) {
            throw new WriteException(e);
        }
        String name = path.getFileName().toString();
        return new Repository(name,
                              path,
                              executor,
                              replicationService,
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
     * @param executor Executor service.
     * @param replicationService Replication service.
     * @return Opened repository.
     */
    static Repository open(Path path, ScheduledExecutorService executor, ReplicationService replicationService) {
        if (!Files.isDirectory(path)) {
            throw new InvalidRepositoryPathException();
        }
        String name = path.getFileName().toString();
        return new Repository(name,
                              path,
                              executor,
                              replicationService,
                              ContentManager.open(path.resolve(CONTENT)),
                              Index.open(name, path.resolve(INDEX)));
    }

    /**
     * @return The name of this repository.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The path of this repository.
     */
    public Path getPath() {
        return path;
    }

    /**
     * Close this repository, releasing underlying resources. Does nothing if it already closed. Any latter operation
     * will fail.
     */
    void close() {
        agent.stop();
        index.close();
        storageManager.close();
        contentManager.close();
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
     * Merge a revision tree with existing one, if any. If associated content is not present, started transaction is
     * suspended so that caller may latter complete this operation by creating this content.
     *
     * @param contentInfoTree Revision tree.
     * @return Actual operation result.
     */
    public CommandResult mergeTree(final ContentInfoTree contentInfoTree) {
        LOG.info("[{}] Merging tree for {}", name, contentInfoTree.getContent());
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
        LOG.info("[{}] Adding content {}", name, hash);
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
        LOG.info("[{}] Deleting content {}, with head {}", name, hash, head);
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
            agent.signal();
            replicationService.signal(name);
        }
    }

    /**
     * Provides info tree associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info tree.
     */
    public ContentInfoTree getInfoTree(final Hash hash) {
        LOG.info("[{}] Returning info tree {}", name, hash);
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
     * Provides info head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info head revisions.
     */
    public List<ContentInfo> getInfoHead(final Hash hash) {
        LOG.info("[{}] Returning info head {}", name, hash);
        ContentInfoTree tree = getInfoTree(hash);
        return tree.get(tree.getHead());
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
     * Provides an input stream on a content in this repository.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream getContent(final Hash hash) {
        LOG.info("[{}] Returning content {}", name, hash);
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
        LOG.info("[{}] Returning content {} with head {}", name, hash, head);
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
        LOG.info("[{}] Returning history{}, first {}, count {}", name, chronological ? "" : ", reverse", first, number);
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
        return index.find(query, first, number);
    }
}
