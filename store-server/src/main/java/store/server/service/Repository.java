package store.server.service;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
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
        agent = new IndexingAgent(volume, index);
    }

    /**
     * Create a new repository.
     *
     * @param path Repository home. Expected not to exist.
     * @param replicationService The replication service.
     * @return Created repository.
     */
    static Repository create(Path path, ReplicationService replicationService) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        String name = path.getFileName().toString();
        return new Repository(path,
                              replicationService,
                              Volume.create(name, path.resolve("volume")),
                              Index.create(name, path.resolve("index")));
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
    static Repository open(Path path, ReplicationService replicationService) {
        String name = path.getFileName().toString();
        return new Repository(path,
                              replicationService,
                              Volume.open(name, path.resolve("volume")),
                              Index.open(name, path.resolve("index")));
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
        return new Status(path, volume.isStarted());
    }

    /**
     * Start this repository. Does nothing if it is already started.
     */
    public void start() {
        volume.start();
        replicationService.start(getName());
    }

    /**
     * Stop this repository. Does nothing if it is already stopped.
     */
    public void stop() {
        replicationService.stop(getName());
        volume.stop();
    }

    /**
     * Put an info revision into this repository. If associated content is not present, started transaction is suspended
     * so that caller may latter complete this operation by creating this content.
     *
     * @param contentInfo Content info revision.
     * @return Actual operation result.
     */
    public CommandResult put(ContentInfo contentInfo) {
        CommandResult result = volume.put(contentInfo);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    /**
     * Put a revision tree into this repository. If associated content is not present, started transaction is suspended
     * so that caller may latter complete this operation by creating this content.
     *
     * @param contentInfoTree Revision tree.
     * @return Actual operation result.
     */
    public CommandResult put(ContentInfoTree contentInfoTree) {
        CommandResult result = volume.put(contentInfoTree);
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
        CommandResult result = volume.create(transactionId, hash, source);
        agent.signal();
        replicationService.signal(getName());
        return result;
    }

    /**
     * Delete a content from this repository.
     *
     * @param hash Hash of the content to delete.
     * @param head Hashes of expected head revisions of the info associated with the content.
     * @return Actual operation result.
     */
    public CommandResult delete(Hash hash, SortedSet<Hash> head) {
        CommandResult result = volume.delete(hash, head);
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
        return volume.getInfoTree(hash);

    }

    /**
     * Provides info head revisions associated with supplied hash.
     *
     * @param hash Hash of the content.
     * @return Corresponding info head revisions.
     */
    public List<ContentInfo> getInfoHead(Hash hash) {
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
        return volume.getInfoRevisions(hash, revs);
    }

    /**
     * Provides an input stream on a content in this repository.
     *
     * @param hash Hash of the content.
     * @return An input stream on this content.
     */
    public InputStream getContent(Hash hash) {
        return volume.getContent(hash);
    }

    /**
     * Provides an input stream on a content if its info head matches supplied one.
     *
     * @param hash Hash of the content.
     * @param head Hashes of expected head revisions of the info associated with the content.
     * @return An input stream on this content, or nothing if supplied head has been superseded.
     */
    public Optional<InputStream> getContent(final Hash hash, final SortedSet<Hash> head) {
        return volume.getContent(hash, head);
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
        return index.find(query, first, number);
    }
}
