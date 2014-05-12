package store.server.service;

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
import store.common.ReplicationDef;
import store.common.RepositoryDef;
import store.server.exception.RepositoryClosedException;
import store.server.exception.SelfReplicationException;
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
    private final ReplicationService replicationService = new ReplicationService();
    private final StorageService storageService;
    private final StorageManager storageManager;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home The repositories service home directory.
     */
    public RepositoriesService(Path home) {
        Path storage = home.resolve(STORAGE);
        try {
            if (!Files.exists(storage)) {
                Files.createDirectory(storage);
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        storageManager = new StorageManager(RepositoriesService.class.getSimpleName(), storage);
        storageService = new StorageService(storageManager);

        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                for (RepositoryDef def : storageService.listRepositoryDefs()) {
                    Repository repository = Repository.open(def.getPath(), replicationService);
                    repositories.put(repository.getName(), repository);
                }
                for (ReplicationDef def : storageService.listReplicationDefs()) {
                    replicationService.createReplication(repositories.get(def.getSource()),
                                                         repositories.get(def.getDestination()));
                }
            }
        });
    }

    /**
     * Close all managed repositories and stop all replications, releasing underlying resources. Does nothing if it
     * already closed. Any latter operation will fail.
     */
    public void close() {
        lock.writeLock().lock();
        try {
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
                    String name = path.getFileName().toString();
                    storageService.createRepositoryDef(new RepositoryDef(name, path));
                    repositories.put(name, Repository.create(path, replicationService));
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Open an existing repository.
     *
     * @param name Repository name.
     */
    public void openRepository(final String name) {
        LOG.info("Opening repository {}", name);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    // First load definition to ensure that repository actually exists.
                    RepositoryDef repositoryDef = storageService.getRepositoryDef(name);
                    if (repositories.containsKey(name)) {
                        return;
                    }
                    Repository repository = Repository.open(repositoryDef.getPath(), replicationService);
                    repositories.put(repository.getName(), repository);

                    for (ReplicationDef def : storageService.listReplicationDefs()) {
                        if ((def.getSource().equals(name) || def.getDestination().equals(name)) &&
                                repositories.containsKey(def.getSource()) &&
                                repositories.containsKey(def.getDestination())) {

                            replicationService.createReplication(repositories.get(def.getSource()),
                                                                 repositories.get(def.getDestination()));
                        }
                    }
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close an existing repository.
     *
     * @param name Repository name.
     */
    public void closeRepository(final String name) {
        LOG.info("Closing repository {}", name);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    // Load definition to ensure that repository actually exists.
                    storageService.getRepositoryDef(name);
                    if (!repositories.containsKey(name)) {
                        return;
                    }
                    replicationService.dropReplications(name);
                    repositories.remove(name).close();
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop an existing repository.
     *
     * @param name Repository name.
     */
    public void dropRepository(final String name) {
        LOG.info("Dropping repository {}", name);
        lock.writeLock().lock();
        try {
            final Path path = storageManager.inTransaction(new Query<Path>() {
                @Override
                public Path apply() {
                    RepositoryDef def = storageService.getRepositoryDef(name);

                    replicationService.dropReplications(name);
                    repositories.remove(name).close();

                    storageService.deleteRepositoryDef(name);
                    storageService.deleteAllReplicationDefs(name);
                    return def.getPath();
                }
            });
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

    /**
     * Create a new replication from source to destination. Does nothing if such a replication already exist.
     *
     * @param source Source repository name.
     * @param destination Destination repository name.
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
                    // Load definitions to ensure that repositories actually exist.
                    storageService.getRepositoryDef(source);
                    storageService.getRepositoryDef(destination);

                    storageService.createReplicationDef(new ReplicationDef(source, destination));
                    if (repositories.containsKey(source) && repositories.containsKey(destination)) {
                        replicationService.createReplication(repositories.get(source), repositories.get(destination));
                    }
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop an existing replication from source to destination. Does nothing if such a replication do not exist.
     *
     * @param source Source repository name.
     * @param destination Destination repository name.
     */
    public void dropReplication(final String source, final String destination) {
        LOG.info("Dropping replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    storageService.getRepositoryDef(source);
                    storageService.getRepositoryDef(destination);

                    storageService.deleteReplicationDef(source, destination);
                    replicationService.dropReplication(source, destination);
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Provides a snapshot of all currently defined repositories.
     *
     * @return A list of repository definitions.
     */
    public List<RepositoryDef> listRepositoryDefs() {
        LOG.info("Returning repository definitions");
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<List<RepositoryDef>>() {
                @Override
                public List<RepositoryDef> apply() {
                    return storageService.listRepositoryDefs();
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides a snapshot of all currently defined replications.
     *
     * @return A list of replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs() {
        LOG.info("Returning replication definitions");
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<List<ReplicationDef>>() {
                @Override
                public List<ReplicationDef> apply() {
                    return storageService.listReplicationDefs();
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides the definition of an existing repository.
     *
     * @param name Repository name.
     * @return Corresponding repository definition.
     */
    public RepositoryDef getRepositoryDef(final String name) {
        LOG.info("Returning repository definition of {}", name);
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<RepositoryDef>() {
                @Override
                public RepositoryDef apply() {
                    return storageService.getRepositoryDef(name);
                }
            });
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * Provides repository associated with supplied name.
     *
     * @param name A repository name
     * @return Corresponding repository
     */
    public Repository getRepository(final String name) {
        lock.readLock().lock();
        try {
            return storageManager.inTransaction(new Query<Repository>() {
                @Override
                public Repository apply() {
                    storageService.getRepositoryDef(name);
                    if (!repositories.containsKey(name)) {
                        throw new RepositoryClosedException();
                    }
                    return repositories.get(name);
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }
}
