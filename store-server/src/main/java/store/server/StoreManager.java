package store.server;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import store.common.Config;
import store.common.hash.Hash;
import store.common.info.ContentInfo;
import store.common.json.JsonCodec;
import static store.common.json.JsonCodec.encode;
import store.server.exception.NoStoreException;
import store.server.exception.StoreAlreadyExists;

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

    private Optional<Config> loadConfig() {
        Path path = home.resolve(CONFIG_PATH);
        if (!Files.exists(path)) {
            return Optional.absent();
        }
        try (InputStream inputStream = Files.newInputStream(path);
                Reader reader = new InputStreamReader(inputStream, Charsets.UTF_8);
                JsonReader jsonReader = Json.createReader(reader)) {

            return Optional.of(JsonCodec.decodeConfig(jsonReader.readObject()));

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

                jsonWriter.writeObject(encode(config));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
