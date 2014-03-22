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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.Config;
import static store.common.json.JsonUtil.readConfig;
import static store.common.json.JsonUtil.writeConfig;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.SelfReplicationException;
import store.server.exception.UnknownRepositoryException;
import store.server.exception.WriteException;

/**
 * Manage a set of repositories, with asynchronous replication support.
 */
public final class RepositoriesService {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesService.class);
    private final Path home;
    private final ReplicationService replicationService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final Map<String, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home The repositories service home directory.
     * @param replicationService The replication service.
     */
    public RepositoriesService(Path home, ReplicationService replicationService) {
        this.home = home;
        this.replicationService = replicationService;
        config = loadConfig();
        for (Path path : config.getRepositories()) {
            Repository repository = Repository.open(path, replicationService);
            repositories.put(repository.getName(), repository);
        }
        for (Repository source : repositories.values()) {
            for (String destinationName : config.getSync(source.getName())) {
                Repository destination = repositories.get(destinationName);
                ReplicationAgent agent = new ReplicationAgent(source, destination);
                replicationService.sync(source.getName(), destinationName, agent);
            }
        }
    }

    public void createRepository(Path path) {
        LOG.info("Creating repository at {}", path);
        lock.writeLock().lock();
        try {
            if (repositories.containsKey(path.getFileName().toString())) {
                throw new RepositoryAlreadyExistsException();
            }
            Repository repository = Repository.create(path, replicationService);
            config.addRepository(path);
            saveConfig();
            repositories.put(repository.getName(), repository);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropRepository(String name) {
        LOG.info("Dropping repository {}", name);
        lock.writeLock().lock();
        try {
            if (!repositories.containsKey(name)) {
                throw new UnknownRepositoryException();
            }
            Path path = repositories.get(name).getPath();

            config.removeRepository(name);
            replicationService.drop(name);
            repositories.remove(name).stop();
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

    public void sync(String source, String destination) {
        LOG.info("Syncing {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            if (!repositories.containsKey(source) || !repositories.containsKey(destination)) {
                throw new UnknownRepositoryException();
            }
            if (source.equals(destination)) {
                throw new SelfReplicationException();
            }
            config.sync(source, destination);
            saveConfig();
            ReplicationAgent agent = new ReplicationAgent(repositories.get(source), repositories.get(destination));
            replicationService.sync(source, destination, agent);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void unsync(String source, String destination) {
        LOG.info("Unsyncing {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            if (!repositories.containsKey(source) || !repositories.containsKey(destination)) {
                throw new UnknownRepositoryException();
            }
            config.unsync(source, destination);
            saveConfig();
            replicationService.unsync(source, destination);

        } finally {
            lock.writeLock().unlock();
        }
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

    public Repository getRepository(String name) {
        lock.readLock().lock();
        try {
            if (!repositories.containsKey(name)) {
                throw new UnknownRepositoryException();
            }
            return repositories.get(name);

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
