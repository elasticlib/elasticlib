package store.server.service;

import com.google.common.base.Predicate;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
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
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.SelfReplicationException;
import store.server.exception.UnknownRepositoryException;
import store.server.exception.WriteException;

/**
 * Manage repositories and replication between them.
 */
public class RepositoriesService {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesService.class);
    private final ReplicationService replicationService = new ReplicationService();
    private final StorageService storageService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, Repository> repositories = new HashMap<>();

    /**
     * Constructor.
     *
     * @param home The repositories service home directory.
     */
    public RepositoriesService(Path home) {
        storageService = new StorageService(home);
        for (RepositoryDef def : storageService.loadAll(RepositoryDef.class)) {
            Repository repository = Repository.open(def.getPath(), replicationService);
            repositories.put(repository.getName(), repository);
        }
        for (ReplicationDef def : storageService.loadAll(ReplicationDef.class)) {
            Repository source = repositories.get(def.getSource());
            Repository destination = repositories.get(def.getDestination());
            ReplicationAgent agent = new ReplicationAgent(source, destination);
            replicationService.createReplication(source.getName(), destination.getName(), agent);
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
            if (repositories.containsKey(path.getFileName().toString())) {
                throw new RepositoryAlreadyExistsException();
            }
            Repository repository = Repository.create(path, replicationService);
            RepositoryDef def = new RepositoryDef(repository.getName(), path);
            storageService.add(RepositoryDef.class, def);
            repositories.put(repository.getName(), repository);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Drop an existing repository.
     *
     * @param name Repository name
     */
    public void dropRepository(final String name) {
        LOG.info("Dropping repository {}", name);
        lock.writeLock().lock();
        try {
            if (!repositories.containsKey(name)) {
                throw new UnknownRepositoryException();
            }
            Path path = repositories.get(name).getPath();
            RepositoryDef repositoryDef = new RepositoryDef(name, path);
            List<ReplicationDef> replicationDefs = newArrayList(filter(storageService.loadAll(ReplicationDef.class),
                                                                       new Predicate<ReplicationDef>() {
                @Override
                public boolean apply(ReplicationDef def) {
                    return !def.getSource().equals(name) && !def.getDestination().equals(name);
                }
            }));
            replicationService.dropReplications(name);
            repositories.remove(name).stop();
            storageService.saveAll(ReplicationDef.class, replicationDefs);
            storageService.remove(RepositoryDef.class, repositoryDef);
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
    public void createReplication(String source, String destination) {
        LOG.info("Creating replication {}>{}", source, destination);
        lock.writeLock().lock();
        try {
            if (!repositories.containsKey(source) || !repositories.containsKey(destination)) {
                throw new UnknownRepositoryException();
            }
            if (source.equals(destination)) {
                throw new SelfReplicationException();
            }
            ReplicationDef def = new ReplicationDef(source, destination);
            if (!storageService.add(ReplicationDef.class, def)) {
                return;
            }
            ReplicationAgent agent = new ReplicationAgent(repositories.get(source), repositories.get(destination));
            replicationService.createReplication(source, destination, agent);

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
    public void dropReplication(String source, String destination) {
        LOG.info("Dropping replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            if (!repositories.containsKey(source) || !repositories.containsKey(destination)) {
                throw new UnknownRepositoryException();
            }
            ReplicationDef def = new ReplicationDef(source, destination);
            if (!storageService.remove(ReplicationDef.class, def)) {
                return;
            }
            replicationService.dropReplication(source, destination);

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
            return storageService.loadAll(RepositoryDef.class);

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
            return storageService.loadAll(ReplicationDef.class);

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
}
