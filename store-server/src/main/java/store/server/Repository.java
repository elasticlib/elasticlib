package store.server;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import store.common.Operation;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.WriteException;
import store.server.volume.Volume;

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
        agent = new IndexingAgent(volume, index);
    }

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

    public static Repository open(Path path, ReplicationService replicationService) {
        return new Repository(path,
                              replicationService,
                              Volume.open(path.resolve("volume")),
                              Index.open(path.resolve("index")));
    }

    public String getName() {
        return path.getFileName().toString();
    }

    public Path getPath() {
        return path;
    }

    public Status getStatus() {
        LOG.info("[{}] Returning status", getName());
        return new Status(path, volume.isStarted());
    }

    public void start() {
        LOG.info("[{}] Starting", getName());
        volume.start();
        replicationService.start(getName());
    }

    public void stop() {
        LOG.info("[{}] Stopping", getName());
        replicationService.stop(getName());
        volume.stop();
    }

    public CommandResult put(ContentInfo contentInfo, InputStream source, RevSpec revSpec) {
        LOG.info("[{}] Putting info and content for {}, with spec {}", getName(), contentInfo.getHash(), revSpec);
        CommandResult result = volume.put(contentInfo, source, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    public CommandResult put(ContentInfoTree contentInfoTree, InputStream source, RevSpec revSpec) {
        LOG.info("[{}] Putting tree and content for {}, with spec {}", getName(), contentInfoTree.getHash(), revSpec);
        CommandResult result = volume.put(contentInfoTree, source, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    public CommandResult put(ContentInfo contentInfo, RevSpec revSpec) {
        LOG.info("[{}] Putting info for {}, with spec {}", getName(), contentInfo.getHash(), revSpec);
        CommandResult result = volume.put(contentInfo, revSpec);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    public CommandResult put(ContentInfoTree contentInfoTree, RevSpec revSpec) {
        LOG.info("[{}] Putting tree for {}, with spec {}", getName(), contentInfoTree.getHash(), revSpec);
        CommandResult result = volume.put(contentInfoTree, revSpec);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    public CommandResult create(int transactionId, Hash hash, InputStream source) {
        LOG.info("[{}] Creating content {}", getName(), hash);
        CommandResult result = volume.create(transactionId, hash, source);
        agent.signal();
        replicationService.signal(getName());
        return result;
    }

    public CommandResult delete(Hash hash, RevSpec revSpec) {
        LOG.info("[{}] Deleting content {}, with spec {}", getName(), hash, revSpec);
        CommandResult result = volume.delete(hash, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
            replicationService.signal(getName());
        }
        return result;
    }

    public ContentInfoTree getInfoTree(Hash hash) {
        LOG.info("[{}] Returning info tree {}", getName(), hash);
        return volume.getInfoTree(hash);

    }

    public List<ContentInfo> getInfoHead(Hash hash) {
        LOG.info("[{}] Returning info head {}", getName(), hash);
        return volume.getInfoHead(hash);
    }

    public List<ContentInfo> getInfoRevisions(Hash hash, List<Hash> revs) {
        LOG.info("[{}] Returning info revs {} [{}]", getName(), hash, Joiner.on(", ").join(revs));
        return volume.getInfoRevisions(hash, revs);
    }

    public InputStream get(Hash hash) {
        LOG.info("[{}] Returning content {}", getName(), hash);
        return volume.get(hash);
    }

    public List<Event> history(boolean chronological, long first, int number) {
        LOG.info("[{}] Returning history{}, first {}, count {}",
                 getName(), chronological ? "" : ", reverse", first, number);
        return volume.history(chronological, first, number);
    }

    public List<Hash> find(String query, int first, int number) {
        LOG.info("[{}] Finding {}, first {}, count {}", getName(), query, first, number);
        return index.find(query, first, number);
    }
}
