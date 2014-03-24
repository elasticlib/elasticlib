package store.server.service;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import store.common.IndexEntry;
import store.common.Operation;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.WriteException;

/**
 * Composition of a volume and an index. Stores contents with their metadata and performs their asynchronous indexation
 * transparently.
 */
public class Repository {

    private static final Logger LOG = LoggerFactory.getLogger(Repository.class);
    private final Path path;
    private final ReplicationService replicationService;
    private final Volume volume;
    private final Index index;
    private final IndexingAgent agent;

    private Repository(Path path, ReplicationService replicationService, Volume volume, Index index) {
        this.path = path;
        this.replicationService = replicationService;
        this.volume = volume;
        this.index = index;
        agent = new IndexingAgent(path.getFileName().toString(), volume, index);
    }

    /**
     * Create a new repository.
     *
     * @param path Repository home.
     * @param replicationService The replication service.
     * @return Created repository.
     */
    public static Repository create(Path path, ReplicationService replicationService) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        return new Repository(path,
                              replicationService,
                              Volume.create(path.resolve("volume")),
                              Index.create(path.resolve("index")));
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
     * @param replicationService The replication service.
     * @return Opened repository.
     */
    public static Repository open(Path path, ReplicationService replicationService) {
        return new Repository(path,
                              replicationService,
                              Volume.open(path.resolve("volume")),
                              Index.open(path.resolve("index")));
    }

    /**
     * @return The name of this repository.
     */
    public String getName() {
        return path.getFileName().toString();
    }

    /**
     * @return The path of this repository.
     */
    public Path getPath() {
        return path;
    }

    /**
     * @return The status of this repository.
     */
    public Status getStatus() {
        LOG.info("[{}] Returning status", getName());
        return new Status(path, volume.isStarted());
    }

    /**
     * Start this repository.
     */
    public void start() {
        LOG.info("[{}] Starting", getName());
        volume.start();
        replicationService.start(getName());
    }

    /**
     * Stop this repository.
     */
    public void stop() {
        LOG.info("[{}] Stopping", getName());
        replicationService.stop(getName());
        volume.stop();
    }

    /**
     * Put a new content into this repository, along with a single info revision.
     *
     * @param contentInfo Content info revision.
     * @param source Content.
     * @param revSpec Expectations on current state for this content.
     * @return Actual operation result.
     */
    public CommandResult put(ContentInfo contentInfo, InputStream source, RevSpec revSpec) {
        LOG.info("[{}] Putting info and content for {}, with spec {}", getName(), contentInfo.getHash(), revSpec);
        CommandResult result = volume.put(contentInfo, source, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    /**
     * Put a new content into this repository, along with a related revision tree.
     *
     * @param contentInfoTree Revision tree.
     * @param source Content.
     * @param revSpec Expectations on current state for this content.
     * @return Actual operation result.
     */
    public CommandResult put(ContentInfoTree contentInfoTree, InputStream source, RevSpec revSpec) {
        LOG.info("[{}] Putting tree and content for {}, with spec {}", getName(), contentInfoTree.getHash(), revSpec);
        CommandResult result = volume.put(contentInfoTree, source, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    /**
     * Put an info revision into this repository.
     *
     * @param contentInfo Content info revision.
     * @param revSpec Expectations on current state for this content.
     * @return Actual operation result.
     */
    public CommandResult put(ContentInfo contentInfo, RevSpec revSpec) {
        LOG.info("[{}] Putting info for {}, with spec {}", getName(), contentInfo.getHash(), revSpec);
        CommandResult result = volume.put(contentInfo, revSpec);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    /**
     * Put a revision tree into this repository.
     *
     * @param contentInfoTree Revision tree.
     * @param revSpec Expectations on current state for this content.
     * @return Actual operation result.
     */
    public CommandResult put(ContentInfoTree contentInfoTree, RevSpec revSpec) {
        LOG.info("[{}] Putting tree for {}, with spec {}", getName(), contentInfoTree.getHash(), revSpec);
        CommandResult result = volume.put(contentInfoTree, revSpec);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    /**
     * Resume a previously suspended transaction and create content from supplied input-stream.
     *
     * @param transactionId Identifier of the transaction to resume.
     * @param hash Content hash. Used to retrieve its associated info.
     * @param source Content.
     * @return Operation result.
     */
    public CommandResult create(int transactionId, Hash hash, InputStream source) {
        LOG.info("[{}] Creating content {}", getName(), hash);
        CommandResult result = volume.create(transactionId, hash, source);
        agent.signal();
        replicationService.signal(getName());
        return result;
    }

    /**
     * Delete a content from this repository.
     *
     * @param hash Hash of the content to delete.
     * @param revSpec Expectations on current state for this content.
     * @return Actual operation result.
     */
    public CommandResult delete(Hash hash, RevSpec revSpec) {
        LOG.info("[{}] Deleting content {}, with spec {}", getName(), hash, revSpec);
        CommandResult result = volume.delete(hash, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    /**
     * Provides info tree associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info tree.
     */
    public ContentInfoTree getInfoTree(Hash hash) {
        LOG.info("[{}] Returning info tree {}", getName(), hash);
        return volume.getInfoTree(hash);

    }

    /**
     * Provides info head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info head revisions.
     */
    public List<ContentInfo> getInfoHead(Hash hash) {
        LOG.info("[{}] Returning info head {}", getName(), hash);
        return volume.getInfoHead(hash);
    }

    /**
     * Provides requested info revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @param revs Hash of revisions to return.
     * @return Corresponding revisions.
     */
    public List<ContentInfo> getInfoRevisions(Hash hash, Collection<Hash> revs) {
        LOG.info("[{}] Returning info revs {} [{}]", getName(), hash, Joiner.on(", ").join(revs));
        return volume.getInfoRevisions(hash, revs);
    }

    /**
     * Provides an input stream on a content in this repository.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream getContent(Hash hash) {
        LOG.info("[{}] Returning content {}", getName(), hash);
        return volume.getContent(hash);
    }

    /**
     * Provides a paginated view of the history of this repository.
     *
     * @param chronological If true, returned list of events will sorted chronologically.
     * @param first Event sequence identifier to start with.
     * @param number Number of events to return.
     * @return A list of events.
     */
    public List<Event> history(boolean chronological, long first, int number) {
        LOG.info("[{}] Returning history{}, first {}, count {}",
                 getName(), chronological ? "" : ", reverse", first, number);
        return volume.history(chronological, first, number);
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
        LOG.info("[{}] Finding {}, first {}, count {}", getName(), query, first, number);
        return index.find(query, first, number);
    }
}
