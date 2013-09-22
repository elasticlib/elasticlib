package store.server;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import store.common.Config;
import store.common.ContentInfo;
import store.common.Hash;
import static store.common.JsonUtil.readConfig;
import static store.common.JsonUtil.write;
import store.server.exception.NoStoreException;
import store.server.exception.StoreAlreadyExists;
import store.server.exception.WriteException;

public final class StoreManager {

    private static final String CONFIG_PATH = "store.config";
    private final Path home;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Optional<Store> store;

    public StoreManager(Path home) {
        this.home = home;
        Optional<Config> config = loadConfig();
        if (config.isPresent()) {
            store = Optional.of(Store.open(config.get()));
        } else {
            store = Optional.absent();
        }
    }

    public void create(Config config) {
        lock.writeLock().lock();
        try {
            if (store.isPresent()) {
                throw new StoreAlreadyExists();
            }
            store = Optional.of(Store.create(config));
            saveConfig(config);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void drop() {
        lock.writeLock().lock();
        try {
            if (!store.isPresent()) {
                throw new NoStoreException();
            }
            Config config = loadConfig().get();
            for (Path volume : config.getVolumePaths()) {
                recursiveDelete(volume);
            }
            recursiveDelete(config.getRoot());
            Files.delete(home.resolve(CONFIG_PATH));
            store = Optional.absent();

        } catch (IOException e) {
            throw new WriteException(e);

        } finally {
            lock.writeLock().unlock();
        }
    }

    private static void recursiveDelete(Path path) throws IOException {
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
    }

    public void put(ContentInfo contentInfo, InputStream source) {
        lock.readLock().lock();
        try {
            if (!store.isPresent()) {
                throw new NoStoreException();
            }
            store.get().put(contentInfo, source);

        } finally {
            lock.readLock().unlock();
        }
    }

    public void delete(Hash hash) {
        lock.readLock().lock();
        try {
            if (!store.isPresent()) {
                throw new NoStoreException();
            }
            store.get().delete(hash);

        } finally {
            lock.readLock().unlock();
        }
    }

    public ContentInfo info(Hash hash) {
        lock.readLock().lock();
        try {
            if (!store.isPresent()) {
                throw new NoStoreException();
            }
            return store.get().info(hash);

        } finally {
            lock.readLock().unlock();
        }
    }

    public ContentReader get(Hash hash) {
        lock.readLock().lock();
        try {
            if (!store.isPresent()) {
                throw new NoStoreException();
            }
            return store.get().get(hash);

        } finally {
            lock.readLock().unlock();
        }
    }

    private Optional<Config> loadConfig() {
        Path path = home.resolve(CONFIG_PATH);
        if (!Files.exists(path)) {
            return Optional.absent();
        }
        try (InputStream inputStream = Files.newInputStream(path);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                JsonReader jsonReader = Json.createReader(reader)) {

            return Optional.of(readConfig(jsonReader.readObject()));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void saveConfig(Config config) {
        try {
            Path path = home.resolve(CONFIG_PATH);
            try (OutputStream outputStream = Files.newOutputStream(path);
                    Writer writer = new OutputStreamWriter(outputStream, Charsets.UTF_8);
                    JsonWriter jsonWriter = Json.createWriter(writer)) {

                jsonWriter.writeObject(write(config));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
