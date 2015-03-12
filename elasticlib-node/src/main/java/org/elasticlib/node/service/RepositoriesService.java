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
package org.elasticlib.node.service;

import java.nio.file.Path;
import java.util.List;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.common.model.RepositoryInfo;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.message.RepositoryClosed;
import org.elasticlib.node.manager.message.RepositoryOpened;
import org.elasticlib.node.manager.message.RepositoryRemoved;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages repositories.
 */
public class RepositoriesService {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesService.class);

    private final StorageManager storageManager;
    private final MessageManager messageManager;
    private final LocalRepositoriesPool localRepositoriesPool;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param messageManager Messaging infrastructure manager.
     * @param localRepositoriesPool Local repositories pool.
     */
    public RepositoriesService(StorageManager storageManager,
                               MessageManager messageManager,
                               LocalRepositoriesPool localRepositoriesPool) {

        this.storageManager = storageManager;
        this.messageManager = messageManager;
        this.localRepositoriesPool = localRepositoriesPool;
    }

    /**
     * Opens all repositories..
     */
    public void start() {
        storageManager.inTransaction(() -> {
            localRepositoriesPool.start();
        });
    }

    /**
     * Closes all managed repositories, releasing underlying resources.
     */
    public void stop() {
        localRepositoriesPool.stop();
    }

    /**
     * Creates a new repository.
     *
     * @param path Repository home.
     */
    public void createRepository(Path path) {
        LOG.info("Creating repository at {}", path);
        storageManager.inTransaction(() -> {
            localRepositoriesPool.createRepository(path);
        });
    }

    /**
     * Adds an unknown repository.
     *
     * @param path Repository home.
     */
    public void addRepository(Path path) {
        LOG.info("Adding repository at {}", path);
        storageManager.inTransaction(() -> {
            localRepositoriesPool.addRepository(path);
        });
    }

    /**
     * Opens an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void openRepository(String key) {
        LOG.info("Opening repository {}", key);
        RepositoryDef def = storageManager.inTransaction(() -> {
            return localRepositoriesPool.openRepository(key);
        });
        messageManager.post(new RepositoryOpened(def.getGuid()));
    }

    /**
     * Closes an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void closeRepository(String key) {
        LOG.info("Closing repository {}", key);
        RepositoryDef def = storageManager.inTransaction(() -> {
            return localRepositoriesPool.closeRepository(key);
        });
        messageManager.post(new RepositoryClosed(def.getGuid()));
    }

    /**
     * Removes an existing repository, without deleting it.
     *
     * @param key Repository name or encoded GUID.
     */
    public void removeRepository(String key) {
        LOG.info("Removing repository {}", key);
        RepositoryDef def = storageManager.inTransaction(() -> {
            return localRepositoriesPool.removeRepository(key);
        });
        messageManager.post(new RepositoryRemoved(def.getGuid()));
    }

    /**
     * Physically deletes an existing repository.
     *
     * @param key Repository name or encoded GUID.
     */
    public void deleteRepository(String key) {
        LOG.info("Deleting repository {}", key);
        RepositoryDef def = storageManager.inTransaction(() -> {
            return localRepositoriesPool.deleteRepository(key);
        });
        messageManager.post(new RepositoryRemoved(def.getGuid()));
    }

    /**
     * Provides info about all currently defined repositories.
     *
     * @return A list of repository info.
     */
    public List<RepositoryInfo> listRepositoryInfos() {
        LOG.info("Returning repository infos");
        return storageManager.inTransaction(localRepositoriesPool::listRepositoryInfos);
    }

    /**
     * Provides info about an existing repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding repository info.
     */
    public RepositoryInfo getRepositoryInfo(String key) {
        LOG.info("Returning repository info of {}", key);
        return storageManager.inTransaction(() -> localRepositoriesPool.getRepositoryInfo(key));
    }

    /**
     * Provides a repository.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding repository
     */
    public Repository getRepository(String key) {
        return storageManager.inTransaction(() -> localRepositoriesPool.getRepository(key));
    }
}
