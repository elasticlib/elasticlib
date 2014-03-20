package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import store.common.Operation;
import store.server.agent.IndexAgent;
import store.server.exception.InvalidRepositoryPathException;
import store.server.exception.WriteException;
import store.server.volume.Volume;

/**
 * Composition of a volume and an index. Stores contents with their metadata and performs their asynchronous indexation
 * transparently.
 */
public class Repository {

    private final Path path;
    private final Volume volume;
    private final Index index;
    private final IndexAgent agent;

    private Repository(Path path, Volume volume, Index index) {
        this.path = path;
        this.volume = volume;
        this.index = index;
        agent = new IndexAgent(volume, index);
    }

    public static Repository create(Path path) {
        try {
            Files.createDirectories(path);
            if (!isEmptyDir(path)) {
                throw new InvalidRepositoryPathException();
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        return new Repository(path,
                              Volume.create(path.resolve("volume")),
                              Index.create(path.resolve("index")));
    }

    private static boolean isEmptyDir(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            return !stream.iterator().hasNext();
        }
    }

    public static Repository open(Path path) {
        return new Repository(path,
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
        return new Status(path, volume.isStarted());
    }

    public void start() {
        volume.start();
    }

    public void stop() {
        volume.stop();
    }

    public CommandResult put(ContentInfo contentInfo, InputStream source, RevSpec revSpec) {
        CommandResult result = volume.put(contentInfo, source, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
        }
        return result;
    }

    public CommandResult put(ContentInfoTree contentInfoTree, InputStream source, RevSpec revSpec) {
        CommandResult result = volume.put(contentInfoTree, source, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
        }
        return result;
    }

    public CommandResult put(ContentInfo contentInfo, RevSpec revSpec) {
        CommandResult result = volume.put(contentInfo, revSpec);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
        }
        return result;
    }

    public CommandResult put(ContentInfoTree contentInfoTree, RevSpec revSpec) {
        CommandResult result = volume.put(contentInfoTree, revSpec);
        if (!result.isNoOp() && result.getOperation() != Operation.CREATE) {
            agent.signal();
        }
        return result;
    }

    public CommandResult create(int transactionId, Hash hash, InputStream source) {
        CommandResult result = volume.create(transactionId, hash, source);
        agent.signal();
        return result;
    }

    public CommandResult delete(Hash hash, RevSpec revSpec) {
        CommandResult result = volume.delete(hash, revSpec);
        if (!result.isNoOp()) {
            agent.signal();
        }
        return result;
    }

    public ContentInfoTree getInfoTree(Hash hash) {
        return volume.getInfoTree(hash);

    }

    public List<ContentInfo> getInfoHead(Hash hash) {
        return volume.getInfoHead(hash);
    }

    public List<ContentInfo> getInfoRevisions(Hash hash, List<Hash> revs) {
        return volume.getInfoRevisions(hash, revs);
    }

    public InputStream get(Hash hash) {
        return volume.get(hash);
    }

    public List<Event> history(boolean chronological, long first, int number) {
        return volume.history(chronological, first, number);
    }

    public List<Hash> find(String query, int first, int number) {
        return index.find(query, first, number);
    }
}
