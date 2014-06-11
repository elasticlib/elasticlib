package store.server.service;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.AgentInfo;
import store.common.ReplicationDef;
import store.common.ReplicationInfo;
import store.common.RepositoryDef;
import store.common.RepositoryInfo;
import store.common.config.Config;
import store.common.hash.Guid;
import store.server.async.AsyncService;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.RepositoryClosedException;
import store.server.exception.SelfReplicationException;
import store.server.exception.ServerException;
import store.server.exception.WriteException;
import store.server.storage.Procedure;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Manage repositories and replication between them.
 */
public class RepositoriesService {

    private static final String STORAGE = "storage";
    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesService.class);
    private final Config config;
    private final AsyncService asyncService;
    private final StorageManager storageManager;
    private final StorageService storageService;
    private final ReplicationService replicationService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Guid, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home The repositories service home directory.
     * @param config Configuration holder.
     */
    public RepositoriesService(Path home, final Config config) {
        this.config = config;
        asyncService = new AsyncService(config);
        storageManager = newStorageManager(home.resolve(STORAGE), config, asyncService);
        storageService = new StorageService(storageManager);
        replicationService = new ReplicationService(storageManager);

        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                for (RepositoryDef def : storageService.listRepositoryDefs()) {
                    try {
                        openRepository(def);

                    } catch (ServerException e) {
                        LOG.error("Failed to open repository at '" + def.getPath() + "'", e);
                    }
                }
            }
        });
    }

    private static StorageManager newStorageManager(Path path, Config config, AsyncService asyncService) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        return new StorageManager(RepositoriesService.class.getSimpleName(), path, config, asyncService);
    }

    private void openRepository(RepositoryDef repositoryDef) {
        Path path = repositoryDef.getPath();
        Repository repository = Repository.open(path, config, asyncService, replicationService);
        RepositoryDef updatedDef = repository.getDef();
        repositories.put(updatedDef.getGuid(), repository);
        storageService.updateRepositoryDef(updatedDef);

        for (ReplicationDef def : storageService.listReplicationDefs(updatedDef.getGuid())) {
            if (repositories.containsKey(def.getSource()) && repositories.containsKey(def.getDestination())) {
                replicationService.startReplication(repositories.get(def.getSource()),
                                                    repositories.get(def.getDestination()));
            }
        }
    }

    /**
     * Close all managed repositories and stop all replications, releasing underlying resources. Does nothing if it
     * already closed. Any latter operation will fail.
     */
    public void close() {
        lock.writeLock().lock();
        try {
            asyncService.close();
            replicationService.close();
            for (Repository repository : repositories.values()) {
                repository.close();
            }
            storageManager.close();

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
                    Repository repository = Repository.create(path, config, asyncService, replicationService);
                    RepositoryDef def = repository.getDef();
                    storageService.createRepositoryDef(def);
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
                    if (anyRepositoryExistsAt(path)) {
                        throw new RepositoryAlreadyExistsException();
                    }
                    Repository repository = Repository.open(path, config, asyncService, replicationService);
                    RepositoryDef def = repository.getDef();
                    storageService.createRepositoryDef(def);
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
                    RepositoryDef repositoryDef = storageService.getRepositoryDef(key);
                    if (repositories.containsKey(repositoryDef.getGuid())) {
                        return;
                    }
                    openRepository(repositoryDef);
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
                    RepositoryDef def = storageService.getRepositoryDef(key);
                    if (!repositories.containsKey(def.getGuid())) {
                        return;
                    }
                    replicationService.stopReplications(def.getGuid());
                    repositories.remove(def.getGuid()).close();
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void removeRepository(final String key) {
        LOG.info("Removing repository {}", key);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    Guid guid = storageService.getRepositoryDef(key).getGuid();
                    removeRepository(guid);
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Delete an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void deleteRepository(final String key) {
        LOG.info("Dropping repository {}", key);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    RepositoryDef def = storageService.getRepositoryDef(key);
                    removeRepository(def.getGuid());
                    if (!anyRepositoryExistsAt(def.getPath())) {
                        recursiveDelete(def.getPath());
                    }
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void removeRepository(Guid guid) {
        replicationService.dropReplications(guid);
        if (repositories.containsKey(guid)) {
            repositories.remove(guid).close();
        }
        storageService.deleteRepositoryDef(guid);
        storageService.deleteAllReplicationDefs(guid);
    }

    private boolean anyRepositoryExistsAt(Path path) {
        for (RepositoryDef def : storageService.listRepositoryDefs()) {
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
     * Create a new replication from source to destination. Does nothing if such a replication already exist.
     *
     * @param source Source repository name or encoded GUID.
     * @param destination Destination repository name or encoded GUID.
     */
    public void createReplication(final String source, final String destination) {
        LOG.info("Creating replication {}>{}", source, destination);
        if (source.equals(destination)) {
            throw new SelfReplicationException();
        }
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    Guid srcId = storageService.getRepositoryDef(source).getGuid();
                    Guid destId = storageService.getRepositoryDef(destination).getGuid();

                    storageService.createReplicationDef(new ReplicationDef(srcId, destId));
                    if (repositories.containsKey(srcId) && repositories.containsKey(destId)) {
                        replicationService.createReplication(repositories.get(srcId), repositories.get(destId));
                    }
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Delete an existing replication from source to destination. Does nothing if such a replication does not exist.
     *
     * @param source Source repository name or encoded GUID.
     * @param destination Destination repository name or encoded GUID.
     */
    public void deleteReplication(final String source, final String destination) {
        LOG.info("Dropping replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    Guid srcId = storageService.getRepositoryDef(source).getGuid();
                    Guid destId = storageService.getRepositoryDef(destination).getGuid();

                    storageService.deleteReplicationDef(srcId, destId);
                    replicationService.deleteReplication(srcId, destId);
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Start an existing replication from source to destination. If source or destination are not started, first starts
     * them. Does nothing if replication is already started.
     *
     * @param source Source repository name or encoded GUID.
     * @param destination Destination repository name or encoded GUID.
     */
    public void startReplication(final String source, final String destination) {
        LOG.info("Starting replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    RepositoryDef srcDef = storageService.getRepositoryDef(source);
                    RepositoryDef destDef = storageService.getRepositoryDef(destination);
                    if (!repositories.containsKey(srcDef.getGuid())) {
                        openRepository(srcDef);
                    }
                    if (!repositories.containsKey(destDef.getGuid())) {
                        openRepository(destDef);
                    }
                    replicationService.startReplication(repositories.get(srcDef.getGuid()),
                                                        repositories.get(destDef.getGuid()));
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Stop an existing replication from source to destination. Does nothing if such a replication is already stopped.
     *
     * @param source Source repository name or encoded GUID.
     * @param destination Destination repository name or encoded GUID.
     */
    public void stopReplication(final String source, final String destination) {
        LOG.info("Stopping replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    Guid srcId = storageService.getRepositoryDef(source).getGuid();
                    Guid destId = storageService.getRepositoryDef(destination).getGuid();

                    replicationService.stopReplication(srcId, destId);
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Provides info about of all currently defined replications.
     *
     * @return A list of replication info.
     */
    public List<ReplicationInfo> listReplicationInfos() {
        LOG.info("Returning replication infos");
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<List<ReplicationInfo>>() {
                @Override
                public List<ReplicationInfo> apply() {
                    List<ReplicationInfo> list = new ArrayList<>();
                    Map<Guid, RepositoryDef> defs = repositoryDefs();
                    for (ReplicationDef replicationDef : storageService.listReplicationDefs()) {
                        Guid srcId = replicationDef.getSource();
                        Guid destId = replicationDef.getDestination();
                        Optional<AgentInfo> agentInfo = replicationService.getInfo(srcId, destId);
                        if (!agentInfo.isPresent()) {
                            list.add(new ReplicationInfo(defs.get(srcId), defs.get(destId)));
                        } else {
                            list.add(new ReplicationInfo(defs.get(srcId), defs.get(destId), agentInfo.get()));
                        }
                    }
                    return list;
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    private Map<Guid, RepositoryDef> repositoryDefs() {
        return Maps.uniqueIndex(storageService.listRepositoryDefs(), new Function<RepositoryDef, Guid>() {
            @Override
            public Guid apply(RepositoryDef def) {
                return def.getGuid();
            }
        });
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
                    return Lists.transform(storageService.listRepositoryDefs(),
                                           new Function<RepositoryDef, RepositoryInfo>() {
                        @Override
                        public RepositoryInfo apply(RepositoryDef def) {
                            return repositoryInfoOf(def);
                        }
                    });
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
                    return repositoryInfoOf(storageService.getRepositoryDef(key));
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
                    RepositoryDef def = storageService.getRepositoryDef(key);
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
}
