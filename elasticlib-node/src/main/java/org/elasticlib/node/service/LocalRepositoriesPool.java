package org.elasticlib.node.service;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.exception.RepositoryAlreadyClosedException;
import org.elasticlib.common.exception.RepositoryAlreadyExistsException;
import org.elasticlib.common.exception.RepositoryAlreadyOpenException;
import org.elasticlib.common.exception.RepositoryClosedException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages local repositories.
 */
public class LocalRepositoriesPool {

    private static final Logger LOG = LoggerFactory.getLogger(LocalRepositoriesPool.class);

    private final RepositoriesDao repositoriesDao;
    private final LocalRepositoriesFactory factory;
    private final Map<Guid, Repository> repositories = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param repositoriesDao Repositories definitions DAO.
     * @param localRepositoriesFactory Local repositories factory
     */
    public LocalRepositoriesPool(RepositoriesDao repositoriesDao, LocalRepositoriesFactory localRepositoriesFactory) {
        this.repositoriesDao = repositoriesDao;
        this.factory = localRepositoriesFactory;
    }

    /**
     * Opens all repositories.
     */
    public void start() {
        lock.writeLock().lock();
        try {
            repositoriesDao.listRepositoryDefs().forEach(def -> {
                try {
                    openRepository(def);

                } catch (NodeException e) {
                    LOG.error("Failed to open repository at '" + def.getPath() + "'", e);
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void openRepository(RepositoryDef repositoryDef) {
        Path path = Paths.get(repositoryDef.getPath());
        Repository repository = factory.open(path);
        RepositoryDef updatedDef = repository.getDef();
        repositories.put(updatedDef.getGuid(), repository);
        repositoriesDao.updateRepositoryDef(updatedDef);
    }

    /**
     * Closes all managed repositories, releasing underlying resources.
     */
    public void stop() {
        lock.writeLock().lock();
        try {
            repositories.values().forEach(Repository::close);
            repositories.clear();

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Creates a new repository.
     *
     * @param path Repository home.
     */
    public void createRepository(Path path) {
        lock.writeLock().lock();
        try {
            if (hasRepositoryAt(path)) {
                throw new RepositoryAlreadyExistsException();
            }
            Repository repository = factory.create(path);
            RepositoryDef def = repository.getDef();
            repositoriesDao.createRepositoryDef(def);
            repositories.put(def.getGuid(), repository);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Adds an unknown repository.
     *
     * @param path Repository home.
     */
    public void addRepository(Path path) {
        lock.writeLock().lock();
        try {
            if (hasRepositoryAt(path)) {
                throw new RepositoryAlreadyExistsException();
            }
            Repository repository = factory.open(path);
            RepositoryDef def = repository.getDef();
            repositoriesDao.createRepositoryDef(def);
            repositories.put(def.getGuid(), repository);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Opens an existing repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Repository definition.
     */
    public RepositoryDef openRepository(String key) {
        lock.writeLock().lock();
        try {
            RepositoryDef def = repositoriesDao.getRepositoryDef(key);
            if (repositories.containsKey(def.getGuid())) {
                throw new RepositoryAlreadyOpenException();
            }
            openRepository(def);
            return def;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Closes an existing repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Repository definition.
     */
    public RepositoryDef closeRepository(String key) {
        lock.writeLock().lock();
        try {
            RepositoryDef def = repositoriesDao.getRepositoryDef(key);
            if (!repositories.containsKey(def.getGuid())) {
                throw new RepositoryAlreadyClosedException();
            }
            repositories.remove(def.getGuid()).close();
            return def;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes an existing repository, without deleting it.
     *
     * @param key Repository name or encoded GUID.
     * @return Repository definition.
     */
    public RepositoryDef removeRepository(String key) {
        return removeRepository(key, false);
    }

    /**
     * Physically deletes an existing repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Repository definition.
     */
    public RepositoryDef deleteRepository(String key) {
        return removeRepository(key, true);
    }

    private RepositoryDef removeRepository(String key, boolean delete) {
        lock.writeLock().lock();
        try {
            RepositoryDef def = repositoriesDao.getRepositoryDef(key);
            Guid guid = def.getGuid();
            Path path = Paths.get(def.getPath());

            if (repositories.containsKey(guid)) {
                repositories.remove(guid).close();
            }
            repositoriesDao.deleteRepositoryDef(guid);

            if (delete && !hasRepositoryAt(path)) {
                recursiveDelete(path);
            }
            return def;

        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean hasRepositoryAt(Path path) {
        return repositoriesDao.listRepositoryDefs().stream().anyMatch(def -> def.getPath().equals(path.toString()));
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
     * Provides definition of all currently defined repositories.
     *
     * @return A list of repository info.
     */
    public List<RepositoryDef> listRepositoryDefs() {
        lock.readLock().lock();
        try {
            return repositoriesDao.listRepositoryDefs();

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides info about all currently defined repositories.
     *
     * @return A list of repository info.
     */
    public List<RepositoryInfo> listRepositoryInfos() {
        lock.readLock().lock();
        try {
            return unmodifiableList(repositoriesDao.listRepositoryDefs()
                    .stream()
                    .map(this::repositoryInfoOf)
                    .collect(toList()));

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides definition of an existing repository. Fails it if it does not exist.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding RepositoryDef.
     */
    public RepositoryDef getRepositoryDef(String key) {
        lock.readLock().lock();
        try {
            return repositoriesDao.getRepositoryDef(key);

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
        lock.readLock().lock();
        try {
            return repositoryInfoOf(repositoriesDao.getRepositoryDef(key));

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
            return repositoryOf(repositoriesDao.getRepositoryDef(key));

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides a repository.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository.
     */
    public Repository getRepository(Guid guid) {
        lock.readLock().lock();
        try {
            return repositoryOf(repositoriesDao.getRepositoryDef(guid));

        } finally {
            lock.readLock().unlock();
        }
    }

    private Repository repositoryOf(RepositoryDef def) {
        if (!repositories.containsKey(def.getGuid())) {
            throw new RepositoryClosedException();
        }
        return repositories.get(def.getGuid());
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
