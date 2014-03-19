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
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.Config;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.Hash;
import static store.common.json.JsonUtil.readConfig;
import static store.common.json.JsonUtil.writeConfig;
import store.server.agent.AgentManager;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.SelfReplicationException;
import store.server.exception.UnknownRepositoryException;
import store.server.exception.WriteException;

/**
 * Manage a set of repositories, with asynchronous replication support.
 */
public final class RepositoryManager {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoryManager.class);
    private final Path home;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Config config;
    private final AgentManager agentManager = new AgentManager();
    private final Map<String, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home The repository home directory.
     */
    public RepositoryManager(Path home) {
        this.home = home;
        config = loadConfig();
        for (Path path : config.getRepositories()) {
            Repository repository = Repository.open(path);
            repositories.put(repository.getName(), repository);
        }
        for (Repository repository : repositories.values()) {
            for (String destination : config.getSync(repository.getName())) {
                agentManager.sync(repository, repositories.get(destination));
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
            Repository repository = Repository.create(path);
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
            agentManager.drop(name);
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
            agentManager.sync(repositories.get(source), repositories.get(destination));

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
            agentManager.unsync(source, destination);

        } finally {
            lock.writeLock().unlock();
        }
    }

    public void start(String name) {
        LOG.info("Starting {}", name);
        repository(name).start();
        agentManager.start(name);
    }

    public void stop(String name) {
        LOG.info("Stopping {}", name);
        agentManager.stop(name);
        repository(name).stop();
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

    public Status status(String name) {
        LOG.info("Returning {} status", name);
        return repository(name).getStatus();
    }

    public void put(String name, ContentInfo contentInfo, InputStream source, RevSpec revSpec) {
        LOG.info("Putting info and content for {}, with spec {}", contentInfo.getHash(), revSpec);
        repository(name).put(contentInfo, source, revSpec);
        agentManager.signal(name);
    }

    public void put(String name, ContentInfoTree contentInfoTree, InputStream source, RevSpec revSpec) {
        LOG.info("Putting tree and content for {}, with spec {}", contentInfoTree.getHash(), revSpec);
        repository(name).put(contentInfoTree, source, revSpec);
        agentManager.signal(name);
    }

    public void put(String name, ContentInfo contentInfo, RevSpec revSpec) {
        LOG.info("Putting info for {}, with spec {}", contentInfo.getHash(), revSpec);
        repository(name).put(contentInfo, revSpec);
        agentManager.signal(name);
    }

    public void put(String name, ContentInfoTree contentInfoTree, RevSpec revSpec) {
        LOG.info("Putting tree for {}, with spec {}", contentInfoTree.getHash(), revSpec);
        repository(name).put(contentInfoTree, revSpec);
        agentManager.signal(name);
    }

    public void create(String name, int transactionId, Hash hash, InputStream source) {
        LOG.info("Creating {}", hash);
        repository(name).create(transactionId, hash, source);
        agentManager.signal(name);
    }

    public void delete(String name, Hash hash, RevSpec revSpec) {
        LOG.info("Deleting {}, with spec {}", hash, revSpec);
        repository(name).delete(hash, revSpec);
        agentManager.signal(name);
    }

    public ContentInfo info(String name, Hash hash) {
        LOG.info("Returning info {}", hash);
        return repository(name).info(hash);
    }

    public InputStream get(String name, Hash hash) {
        LOG.info("Getting {}", hash);
        return repository(name).get(hash);
    }

    public List<Hash> find(String name, String query, int first, int number) {
        LOG.info("Finding {}, first {}, count {}", query, first, number);
        return repository(name).find(query, first, number);
    }

    public List<Event> history(String name, boolean chronological, long first, int number) {
        LOG.info("Returning history{}, first {}, count {}", chronological ? "" : ", reverse", first, number);
        return repository(name).history(chronological, first, number);
    }

    private Repository repository(String name) {
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
