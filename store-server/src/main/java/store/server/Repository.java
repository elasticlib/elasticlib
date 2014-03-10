package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
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

    public void put(ContentInfo contentInfo, InputStream source, RevSpec revSpec) {
        volume.put(contentInfo, source, revSpec);
        agent.signal();
    }

    public void put(ContentInfoTree contentInfoTree, InputStream source, RevSpec revSpec) {
        volume.put(contentInfoTree, source, revSpec);
        agent.signal();
    }

    public void put(ContentInfo contentInfo, RevSpec revSpec) {
        volume.put(contentInfo, revSpec);
        agent.signal();
    }

    public void put(ContentInfoTree contentInfoTree, RevSpec revSpec) {
        volume.put(contentInfoTree, revSpec);
        agent.signal();
    }

    public void delete(Hash hash, RevSpec revSpec) {
        volume.delete(hash, revSpec);
        agent.signal();
    }

    public ContentInfo info(Hash hash) {
        return volume.info(hash);
    }

    public void get(Hash hash, OutputStream outputStream) {
        volume.get(hash, outputStream);
    }

    public List<Event> history(boolean chronological, long first, int number) {
        return volume.history(chronological, first, number);
    }

    public List<Hash> find(String query, int first, int number) {
        return index.find(query, first, number);
    }
}
