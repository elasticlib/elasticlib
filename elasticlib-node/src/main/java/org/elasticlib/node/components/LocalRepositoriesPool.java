/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.components;

import com.google.common.base.Splitter;
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
import org.elasticlib.common.exception.UnknownRepositoryException;
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

    private static final String SEPARATOR = ".";
    private static final Logger LOG = LoggerFactory.getLogger(LocalRepositoriesPool.class);

    private final RepositoriesDao repositoriesDao;
    private final LocalRepositoriesFactory factory;
    private final NodeNameProvider nodeNameProvider;
    private final NodeGuidProvider nodeGuidProvider;
    private final Map<Guid, Repository> repositories = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param repositoriesDao Repositories definitions DAO.
     * @param localRepositoriesFactory Local repositories factory
     * @param nodeNameProvider Local node name provider.
     * @param nodeGuidProvider Local node GUID provider.
     */
    public LocalRepositoriesPool(RepositoriesDao repositoriesDao,
                                 LocalRepositoriesFactory localRepositoriesFactory,
                                 NodeNameProvider nodeNameProvider,
                                 NodeGuidProvider nodeGuidProvider) {
        this.repositoriesDao = repositoriesDao;
        this.factory = localRepositoriesFactory;
        this.nodeNameProvider = nodeNameProvider;
        this.nodeGuidProvider = nodeGuidProvider;
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
            RepositoryDef def = getRepositoryDef(key);
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
            RepositoryDef def = getRepositoryDef(key);
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
            RepositoryDef def = getRepositoryDef(key);
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
     * Provides info about an existing repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding repository info.
     */
    public RepositoryInfo getRepositoryInfo(String key) {
        lock.readLock().lock();
        try {
            return repositoryInfoOf(getRepositoryDef(key));

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
            return repositoryOf(getRepositoryDef(key));

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Resolves GUID of an existing repository, if it exists.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding RepositoryDef, if any.
     */
    public Optional<Guid> tryGetRepositoryGuid(String key) {
        lock.readLock().lock();
        try {
            return tryGetRepositoryDef(key).map(RepositoryDef::getGuid);

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides definition of an existing repository, if it exists.
     *
     * @param guid Repository GUID.
     * @return Corresponding RepositoryDef, if any.
     */
    public Optional<RepositoryDef> tryGetRepositoryDef(Guid guid) {
        lock.readLock().lock();
        try {
            return repositoriesDao.tryGetRepositoryDef(guid);

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Provides a repository, if it exists. Fails if it exists but is closed.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository, if any.
     */
    public Optional<Repository> tryGetRepositoryIfOpen(Guid guid) {
        lock.readLock().lock();
        try {
            Optional<RepositoryDef> def = repositoriesDao.tryGetRepositoryDef(guid);
            if (!def.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(repositoryOf(def.get()));

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

    private Repository repositoryOf(RepositoryDef def) {
        if (!repositories.containsKey(def.getGuid())) {
            throw new RepositoryClosedException();
        }
        return repositories.get(def.getGuid());
    }

    private RepositoryDef getRepositoryDef(String key) {
        return tryGetRepositoryDef(key).orElseThrow(UnknownRepositoryException::new);
    }

    private Optional<RepositoryDef> tryGetRepositoryDef(String key) {
        if (!key.contains(SEPARATOR)) {
            return repositoriesDao.tryGetRepositoryDef(key);
        }
        List<String> parts = Splitter.on(SEPARATOR).splitToList(key);
        if (parts.size() != 2 || !matchesNode(parts.get(0))) {
            return Optional.empty();
        }
        return repositoriesDao.tryGetRepositoryDef(parts.get(1));
    }

    private boolean matchesNode(String key) {
        return nodeNameProvider.name().equals(key) || nodeGuidProvider.guid().asHexadecimalString().equals(key);
    }
}
