package store.server.service;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import static com.google.common.collect.Lists.transform;
import java.io.IOException;
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
import store.server.manager.storage.Procedure;
import store.server.manager.storage.Query;
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
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    for (RepositoryDef def : repositoriesDao.listRepositoryDefs()) {
                        try {
                            openRepository(def);

                        } catch (NodeException e) {
                            LOG.error("Failed to open repository at '" + def.getPath() + "'", e);
                        }
                    }
                }
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
            for (Repository repository : repositories.values()) {
                repository.close();
            }
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
    public void createRepository(final Path path) {
        LOG.info("Creating repository at {}", path);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    if (hasRepositoryAt(path)) {
                        throw new RepositoryAlreadyExistsException();
                    }
                    Repository repository = Repository.create(path, config, taskManager, messageManager);
                    RepositoryDef def = repository.getDef();
                    repositoriesDao.createRepositoryDef(def);
                    repositories.put(def.getGuid(), repository);
                }
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
    public void addRepository(final Path path) {
        LOG.info("Adding repository at {}", path);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    if (hasRepositoryAt(path)) {
                        throw new RepositoryAlreadyExistsException();
                    }
                    Repository repository = Repository.open(path, config, taskManager, messageManager);
                    RepositoryDef def = repository.getDef();
                    repositoriesDao.createRepositoryDef(def);
                    repositories.put(def.getGuid(), repository);
                }
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
    public void openRepository(final String key) {
        LOG.info("Opening repository {}", key);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                    if (repositories.containsKey(def.getGuid())) {
                        return;
                    }
                    openRepository(def);
                    messageManager.post(new RepositoryOpened(def.getGuid()));
                }
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
    public void closeRepository(final String key) {
        LOG.info("Closing repository {}", key);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                    if (!repositories.containsKey(def.getGuid())) {
                        return;
                    }
                    repositories.remove(def.getGuid()).close();
                    messageManager.post(new RepositoryClosed(def.getGuid()));
                }
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
    public void removeRepository(final String key) {
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

    private void removeRepository(final String key, final boolean delete) {
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
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
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean hasRepositoryAt(Path path) {
        for (RepositoryDef def : repositoriesDao.listRepositoryDefs()) {
            if (def.getPath().equals(path)) {
                return true;
            }
        }
        return false;
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
            return storageManager.inTransaction(new Query<List<RepositoryInfo>>() {
                @Override
                public List<RepositoryInfo> apply() {
                    return ImmutableList.copyOf(transform(repositoriesDao.listRepositoryDefs(),
                                                          new Function<RepositoryDef, RepositoryInfo>() {
                        @Override
                        public RepositoryInfo apply(RepositoryDef def) {
                            return repositoryInfoOf(def);
                        }
                    }));
                }
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
    public RepositoryInfo getRepositoryInfo(final String key) {
        LOG.info("Returning repository info of {}", key);
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<RepositoryInfo>() {
                @Override
                public RepositoryInfo apply() {
                    return repositoryInfoOf(repositoriesDao.getRepositoryDef(key));
                }
            });
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
    public Repository getRepository(final String key) {
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<Repository>() {
                @Override
                public Repository apply() {
                    RepositoryDef def = repositoriesDao.getRepositoryDef(key);
                    if (!repositories.containsKey(def.getGuid())) {
                        throw new RepositoryClosedException();
                    }
                    return repositories.get(def.getGuid());
                }
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
            return Optional.fromNullable(repositories.get(guid));

        } finally {
            lock.readLock().unlock();
        }
    }
}
