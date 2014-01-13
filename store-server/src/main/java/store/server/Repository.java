package store.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import store.server.agent.IndexAgent;
import store.server.exception.InvalidStorePathException;
import store.server.exception.StoreRuntimeException;
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
                throw new InvalidStorePathException();
            }
        } catch (IOException e) {
            throw new StoreRuntimeException(e);
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

    public void put(ContentInfo contentInfo, InputStream source) {
        volume.put(contentInfo, source);
        agent.signal();
    }

    public void delete(Hash hash) {
        volume.delete(hash);
        agent.signal();
    }

    public boolean contains(Hash hash) {
        return volume.contains(hash);
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

    public List<Hash> find(String query) {
        return index.find(query);
    }
}
