package store.server;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import store.common.Config;
import store.common.ContentInfo;
import store.common.Event;
import store.common.Hash;
import static store.common.JsonUtil.readConfig;
import static store.common.JsonUtil.writeConfig;
import store.common.Uid;
import store.server.exception.ConcurrentOperationException;
import store.server.exception.NoIndexException;
import store.server.exception.NoVolumeException;
import store.server.exception.UnknownHashException;
import store.server.exception.UnknownIndexException;
import store.server.exception.UnknownVolumeException;
import store.server.exception.WriteException;

public final class StoreManager {

    private final Path home;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final AgentManager agentManager = new AgentManager();
    private final Map<Uid, Volume> volumes = new HashMap<>();
    private final Map<Uid, Index> indexes = new HashMap<>();

    public StoreManager(Path home) {
        this.home = home;
        config = loadConfig();
        for (Entry<Uid, Path> entry : config.getVolumes().entrySet()) {
            Volume volume = Volume.open(entry.getValue());
            volumes.put(entry.getKey(), volume);
        }
        for (Entry<Uid, Path> entry : config.getIndexes().entrySet()) {
            Index index = Index.open(entry.getValue());
            indexes.put(entry.getKey(), index);
        }
        for (Uid sourceId : config.getVolumes().keySet()) {
            for (Uid destinationId : config.getSync(sourceId)) {
                agentManager.sync(sourceId, volumes.get(sourceId), destinationId, volumes.get(destinationId));
            }
        }
    }

    public void createVolume(Path path) {
        lock.writeLock().lock();
        try {
            Uid uid = Uid.random();
            Volume volume = Volume.create(path);
            config.addVolume(uid, path);
            saveConfig();
            volumes.put(uid, volume);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropVolume(Uid uid) {
        lock.writeLock().lock();
        try {
            if (!config.getVolumes().containsKey(uid)) {
                throw new UnknownVolumeException();
            }
            Path path = config.getVolumes().get(uid);
            Map<Uid, Path> indexesToDrop = new HashMap<>();
            for (Uid indexId : config.getIndexes(uid)) {
                indexesToDrop.put(indexId, config.getIndexes().get(indexId));
            }

            config.removeVolume(uid);
            saveConfig();

            agentManager.close(uid);
            volumes.remove(uid).close();

            recursiveDelete(path);
            for (Entry<Uid, Path> entry : indexesToDrop.entrySet()) {
                indexes.remove(entry.getKey());
                recursiveDelete(entry.getValue());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void createIndex(Path path, Uid volumeId) {
        lock.writeLock().lock();
        try {
            Uid uid = Uid.random();
            Index index = Index.create(path);
            config.addIndex(uid, path, volumeId);
            saveConfig();
            indexes.put(uid, index);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropIndex(Uid uid) {
        lock.writeLock().lock();
        try {
            if (!config.getIndexes().containsKey(uid)) {
                throw new UnknownIndexException();
            }
            Path path = config.getIndexes().get(uid);
            config.removeIndex(uid);
            saveConfig();
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

    public void setWrite(Uid uid) {
        lock.writeLock().lock();
        try {
            if (!config.getVolumes().containsKey(uid)) {
                throw new UnknownVolumeException();
            }
            config.setWrite(uid);
            saveConfig();

        } finally {
            lock.writeLock().unlock();
        }

    }

    public void unsetWrite() {
        lock.writeLock().lock();
        try {
            config.unsetWrite();
            saveConfig();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setRead(Uid uid) {
        lock.writeLock().lock();
        try {
            if (!config.getVolumes().containsKey(uid)) {
                throw new UnknownVolumeException();
            }
            config.setRead(uid);
            saveConfig();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unsetRead() {
        lock.writeLock().lock();
        try {
            config.unsetRead();
            saveConfig();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setSearch(Uid uid) {
        lock.writeLock().lock();
        try {
            if (!config.getIndexes().containsKey(uid)) {
                throw new UnknownIndexException();
            }
            config.setSearch(uid);
            saveConfig();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unsetSearch() {
        lock.writeLock().lock();
        try {
            config.unsetSearch();
            saveConfig();

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void sync(Uid sourceId, Uid destinationId) {
        lock.writeLock().lock();
        try {
            if (!config.getVolumes().containsKey(sourceId) || !config.getVolumes().containsKey(destinationId)) {
                throw new UnknownVolumeException();
            }
            config.sync(sourceId, destinationId);
            saveConfig();
            agentManager.sync(sourceId, volumes.get(sourceId), destinationId, volumes.get(destinationId));

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unsync(Uid sourceId, Uid destinationId) {
        lock.writeLock().lock();
        try {
            if (!config.getVolumes().containsKey(sourceId) || !config.getVolumes().containsKey(destinationId)) {
                throw new UnknownVolumeException();
            }
            config.unsync(sourceId, destinationId);
            saveConfig();
            agentManager.unsync(sourceId, destinationId);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public Config config() {
        lock.readLock().lock();
        try {
            return config;

        } finally {
            lock.readLock().unlock();
        }
    }

    public void put(final ContentInfo contentInfo, final InputStream source) {
        readLocked(new Command() {
            @Override
            public void apply(Volume volume) {
                volume.put(contentInfo, source);
            }
        });
    }

    public void delete(final Hash hash) {
        readLocked(new Command() {
            @Override
            public void apply(Volume volume) {
                volume.delete(hash);
            }
        });
    }

    public boolean contains(final Hash hash) {
        return readLocked(new Query<Boolean>() {
            @Override
            public Boolean apply(Volume volume) {
                return volume.contains(hash);
            }
        });
    }

    public ContentInfo info(final Hash hash) {
        return readLocked(new Query<ContentInfo>() {
            @Override
            public ContentInfo apply(Volume volume) {
                return volume.info(hash);
            }
        });
    }

    public void get(final Hash hash, final OutputStream outputStream) {
        readLocked(new Query<Void>() {
            @Override
            public Void apply(Volume volume) {
                volume.get(hash, outputStream);
                return null;
            }
        });
    }

    public List<ContentInfo> find(final String query) {
        return readLocked(new Query<List<ContentInfo>>() {
            @Override
            public List<ContentInfo> apply(Volume volume) {
                if (!config.getSearch().isPresent()) {
                    throw new NoIndexException();
                }
                List<Hash> hashes = indexes.get(config.getSearch().get()).find(query);
                List<ContentInfo> results = new ArrayList<>(hashes.size());
                for (Hash hash : hashes) {
                    try {
                        results.add(volume.info(hash));

                    } catch (UnknownHashException | ConcurrentOperationException e) {
                    }
                }
                return results;
            }
        });
    }

    public List<Event> history(final boolean chronological, final long first, final int number) {
        return readLocked(new Query<List<Event>>() {
            @Override
            public List<Event> apply(Volume volume) {
                return volume.history(chronological, first, number);
            }
        });
    }

    private void readLocked(Command command) {
        lock.readLock().lock();
        try {
            if (!config.getWrite().isPresent()) {
                throw new NoVolumeException();
            }
            command.apply(volumes.get(config.getWrite().get()));
            agentManager.signal(config.getWrite().get());

        } finally {
            lock.readLock().unlock();
        }
    }

    private interface Command {

        void apply(Volume volume);
    }

    private <T> T readLocked(Query<T> query) {
        lock.readLock().lock();
        try {
            if (!config.getRead().isPresent()) {
                throw new NoVolumeException();
            }
            return query.apply(volumes.get(config.getRead().get()));

        } finally {
            lock.readLock().unlock();
        }
    }

    private interface Query<T> {

        T apply(Volume volume);
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
