package store.server.service;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static java.util.stream.Collectors.toList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.common.exception.NodeException;
import store.common.exception.RepositoryAlreadyExistsException;
import store.common.exception.RepositoryClosedException;
import store.common.hash.Guid;
import store.common.model.RepositoryDef;
import store.common.model.RepositoryInfo;
import store.server.dao.RepositoriesDao;
import store.server.manager.message.MessageManager;
import store.server.manager.message.RepositoryClosed;
import store.server.manager.message.RepositoryOpened;
import store.server.manager.message.RepositoryRemoved;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.TaskManager;
import store.server.repository.Repository;

/**
 * Manage repositories.
 */
public class RepositoriesService {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesService.class);

    private final Config config;
    private final TaskManager taskManager;
    private final StorageManager storageManager;
    private final MessageManager messageManager;
    private final RepositoriesDao repositoriesDao;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Guid, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param storageManager Persistent storage provider.
     * @param messageManager Messaging infrastructure manager.
     * @param repositoriesDao Repositories definitions DAO.
     */
    public RepositoriesService(Config config,
                               TaskManager taskManager,
                               StorageManager storageManager,
                               MessageManager messageManager,
                               RepositoriesDao repositoriesDao) {
        this.config = config;
        this.taskManager = taskManager;
        this.storageManager = storageManager;
        this.messageManager = messageManager;
        this.repositoriesDao = repositoriesDao;
    }

    /**
     * Open all repositories..
     */
    public void start() {
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                repositoriesDao.listRepositoryDefs().stream().forEach(def -> {
                    try {
                        openRepository(def);

                    } catch (NodeException e) {
                        LOG.error("Failed to open repository at '" + def.getPath() + "'", e);
                    }
                });
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void openRepository(RepositoryDef repositoryDef) {
        Path path = repositoryDef.getPath();
        Repository repository = Repository.open(path, config, taskManager, messageManager);
        RepositoryDef updatedDef = repository.getDef();
        repositories.put(updatedDef.getGuid(), repository);
        repositoriesDao.updateRepositoryDef(updatedDef);
    }

    /**
     * Close all managed repositories, releasing underlying resources.
     */
    public void stop() {
        lock.writeLock().lock();
        try {
            repositories.values().stream().forEach(Repository::close);
            repositories.clear();

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Create a new repository.
     *
     * @param path Repository home.
     */
    public void createRepository(Path path) {
        LOG.info("Creating repository at {}", path);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                if (hasRepositoryAt(path)) {
                    throw new RepositoryAlreadyExistsException();
                }
                Repository repository = Repository.create(path, config, taskManager, messageManager);
                RepositoryDef def = repository.getDef();
                repositoriesDao.createRepositoryDef(def);
                repositories.put(def.getGuid(), repository);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add an unknown repository.
     *
     * @param path Repository home.
     */
    public void addRepository(Path path) {
        LOG.info("Adding repository at {}", path);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                if (hasRepositoryAt(path)) {
                    throw new RepositoryAlreadyExistsException();
                }
                Repository repository = Repository.open(path, config, taskManager, messageManager);
                RepositoryDef def = repository.getDef();
                repositoriesDao.createRepositoryDef(def);
                repositories.put(def.getGuid(), repository);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Open an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void openRepository(String key) {
        LOG.info("Opening repository {}", key);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                if (repositories.containsKey(def.getGuid())) {
                    return;
                }
                openRepository(def);
                messageManager.post(new RepositoryOpened(def.getGuid()));
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void closeRepository(String key) {
        LOG.info("Closing repository {}", key);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                if (!repositories.containsKey(def.getGuid())) {
                    return;
                }
                repositories.remove(def.getGuid()).close();
                messageManager.post(new RepositoryClosed(def.getGuid()));
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove an existing repository, without deleting it.
     *
     * @param key Repository name or encoded GUID.
     */
    public void removeRepository(String key) {
        LOG.info("Removing repository {}", key);
        removeRepository(key, false);
    }

    /**
     * Physically delete an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void deleteRepository(String key) {
        LOG.info("Deleting repository {}", key);
        removeRepository(key, true);
    }

    private void removeRepository(String key, boolean delete) {
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                Guid guid = def.getGuid();
                Path path = def.getPath();

                if (repositories.containsKey(guid)) {
                    repositories.remove(guid).close();
                }
                repositoriesDao.deleteRepositoryDef(guid);

                if (delete && !hasRepositoryAt(path)) {
                    recursiveDelete(path);
                }
                messageManager.post(new RepositoryRemoved(def.getGuid()));
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean hasRepositoryAt(Path path) {
        return repositoriesDao.listRepositoryDefs().stream().anyMatch(def -> def.getPath().equals(path));
    }

    private static void recursiveDelete(Path path) {
        // This also checks if path exists at all.
        if (!Files.isDirectory(path)) {
            return;
        }
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
            LOG.error("Failed to delete " + path, e);
        }
    }

    /**
     * Provides info about all currently defined repositories.
     *
     * @return A list of repository info.
     */
    public List<RepositoryInfo> listRepositoryInfos() {
        LOG.info("Returning repository infos");
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(() -> {
                return unmodifiableList(repositoriesDao.listRepositoryDefs()
                        .stream()
                        .map(this::repositoryInfoOf)
                        .collect(toList()));
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides info about an existing repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding repository info.
     */
    public RepositoryInfo getRepositoryInfo(String key) {
        LOG.info("Returning repository info of {}", key);
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(() -> repositoryInfoOf(repositoriesDao.getRepositoryDef(key)));
        } finally {
            lock.readLock().unlock();
        }
    }

    private RepositoryInfo repositoryInfoOf(RepositoryDef def) {
        if (!repositories.containsKey(def.getGuid())) {
            return new RepositoryInfo(def);
        }
        return repositories.get(def.getGuid()).getInfo();
    }

    /**
     * Provides a repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding repository
     */
    public Repository getRepository(String key) {
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(() -> {
                RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                if (!repositories.containsKey(def.getGuid())) {
                    throw new RepositoryClosedException();
                }
                return repositories.get(def.getGuid());
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides a repository if it exists and is currently opened.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository, if available.
     */
    public Optional<Repository> tryGetRepository(Guid guid) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(repositories.get(guid));

        } finally {
            lock.readLock().unlock();
        }
    }
}
