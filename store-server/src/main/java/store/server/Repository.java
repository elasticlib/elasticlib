package store.server;

import store.server.volume.Status;
import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.Config;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.JsonUtil.readConfig;
import static store.common.JsonUtil.writeConfig;
import store.server.agent.AgentManager;
import store.server.exception.IndexAlreadyExistsException;
import store.server.exception.NoIndexException;
import store.server.exception.NoVolumeException;
import store.server.exception.UnknownIndexException;
import store.server.exception.UnknownVolumeException;
import store.server.exception.VolumeAlreadyExistsException;
import store.server.exception.WriteException;
import store.server.volume.Volume;

/**
 * Represents a repository, which is a consistent set of volumes and indexes, with asynchronous replication support.
 */
public final class Repository {

    private static final Logger LOG = LoggerFactory.getLogger(Repository.class);
    private final Path home;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final AgentManager agentManager = new AgentManager();
    private final Map<String, Volume> volumes = new HashMap<>();
    private final Map<String, Index> indexes = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home The repository home directory.
     */
    public Repository(Path home) {
        this.home = home;
        config = loadConfig();
        for (Path path : config.getVolumes()) {
            Volume volume = Volume.open(path);
            volumes.put(volume.getName(), volume);
        }
        for (Path path : config.getIndexes()) {
            Index index = Index.open(path);
            indexes.put(index.getName(), index);
        }
        for (Volume volume : volumes.values()) {
            for (String destination : config.getSync(volume.getName())) {
                if (volumes.containsKey(destination)) {
                    agentManager.sync(volume, volumes.get(destination));
                } else {
                    agentManager.sync(volume, indexes.get(destination));
                }
            }
        }
    }

    public void createVolume(Path path) {
        LOG.info("Creating volume at {}", path);
        lock.writeLock().lock();
        try {
            if (volumes.containsKey(path.getFileName().toString())) {
                throw new VolumeAlreadyExistsException();
            }
            Volume volume = Volume.create(path);
            config.addVolume(path);
            saveConfig();
            volumes.put(volume.getName(), volume);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropVolume(String name) {
        LOG.info("Dropping volume {}", name);
        lock.writeLock().lock();
        try {
            if (!volumes.containsKey(name)) {
                throw new UnknownVolumeException();
            }
            Path path = volumes.get(name).getPath();
            Set<String> indexesToDrop = config.getIndexes(name);

            config.removeVolume(name);
            saveConfig();

            agentManager.drop(name);
            volumes.remove(name).stop();

            recursiveDelete(path);
            for (String indexName : indexesToDrop) {
                Index index = indexes.remove(indexName);
                recursiveDelete(index.getPath());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void createIndex(Path path, String volumeName) {
        LOG.info("Creating index at {} on {}", path, volumeName);
        lock.writeLock().lock();
        try {
            if (indexes.containsKey(path.getFileName().toString())) {
                throw new IndexAlreadyExistsException();
            }
            Index index = Index.create(path);
            config.addIndex(path, volumeName);
            saveConfig();
            indexes.put(index.getName(), index);
            agentManager.sync(volumes.get(volumeName), index);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropIndex(String name) {
        LOG.info("Dropping index {}", name);
        lock.writeLock().lock();
        try {
            if (!indexes.containsKey(name)) {
                throw new UnknownIndexException();
            }
            Path path = indexes.get(name).getPath();
            config.removeIndex(name);
            saveConfig();
            agentManager.drop(name);
            indexes.remove(name);
            recursiveDelete(path);

        } finally {
            lock.writeLock().unlock();
        }
    }

    private static void recursiveDelete(Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                    if (e == null) {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;

                    } else {
                        throw e;
                    }
                }
            });
        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    public void sync(String source, String destination) {
        LOG.info("Syncing {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            if (!volumes.containsKey(source) || !volumes.containsKey(destination)) {
                throw new UnknownVolumeException();
            }
            config.sync(source, destination);
            saveConfig();
            agentManager.sync(volumes.get(source), volumes.get(destination));

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unsync(String source, String destination) {
        LOG.info("Unsyncing {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            if (!volumes.containsKey(source) || !volumes.containsKey(destination)) {
                throw new UnknownVolumeException();
            }
            config.unsync(source, destination);
            saveConfig();
            agentManager.unsync(source, destination);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void start(String name) {
        LOG.info("Starting {}", name);
        volume(name).start();
        agentManager.start(name);
    }

    public void stop(String name) {
        LOG.info("Stopping {}", name);
        agentManager.stop(name);
        volume(name).stop();
    }

    public Config config() {
        LOG.info("Returning config");
        lock.readLock().lock();
        try {
            return config;

        } finally {
            lock.readLock().unlock();
        }
    }

    public Status volumeStatus(String name) {
        LOG.info("Returning {} status", name);
        return volume(name).getStatus();
    }

    public void put(String name, ContentInfo contentInfo, InputStream source) {
        LOG.info("Putting {}", contentInfo.getHash());
        volume(name).put(contentInfo, source);
        agentManager.signal(name);
    }

    public void delete(String name, Hash hash) {
        LOG.info("Deleting {}", hash);
        volume(name).delete(hash);
        agentManager.signal(name);
    }

    public ContentInfo info(String name, Hash hash) {
        LOG.info("Returning info {}", hash);
        return volume(name).info(hash);
    }

    public void get(String name, Hash hash, OutputStream outputStream) {
        LOG.info("Getting {}", hash);
        volume(name).get(hash, outputStream);
    }

    public List<Hash> find(String name, String query) {
        LOG.info("Finding {}", query);
        return index(name).find(query);
    }

    public List<Event> history(String name, boolean chronological, long first, int number) {
        LOG.info("Returning history{}, first {}, count {}", chronological ? "" : ", reverse", first, number);
        return volume(name).history(chronological, first, number);
    }

    private Volume volume(String name) {
        lock.readLock().lock();
        try {
            if (!volumes.containsKey(name)) {
                throw new NoVolumeException(); // FIXME il faudrait faire des exceptions plus adaptées !
            }
            return volumes.get(name);

        } finally {
            lock.readLock().unlock();
        }
    }

    private Index index(String name) {
        lock.readLock().lock();
        try {
            if (!indexes.containsKey(name)) {
                throw new NoIndexException();
            }
            return indexes.get(name);

        } finally {
            lock.readLock().unlock();
        }
    }

    private Config loadConfig() {
        Path path = home.resolve("config");
        if (!Files.exists(path)) {
            return new Config();
        }
        try (InputStream inputStream = Files.newInputStream(path);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                JsonReader jsonReader = Json.createReader(reader)) {

            return readConfig(jsonReader.readObject());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void saveConfig() {
        try {
            Path path = home.resolve("config");
            try (OutputStream outputStream = Files.newOutputStream(path);
                    Writer writer = new OutputStreamWriter(outputStream, Charsets.UTF_8);
                    JsonWriter jsonWriter = Json.createWriter(writer)) {

                jsonWriter.writeObject(writeConfig(config));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
