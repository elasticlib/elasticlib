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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import org.elasticlib.common.exception.SelfReplicationException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.ReplicationDef;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.node.dao.ReplicationsDao;
import org.elasticlib.node.manager.message.Action;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.message.NewRepositoryEvent;
import org.elasticlib.node.manager.message.RepositoryChangeMessage;
import org.elasticlib.node.manager.message.RepositoryClosed;
import org.elasticlib.node.manager.message.RepositoryOpened;
import org.elasticlib.node.manager.message.RepositoryRemoved;
import org.elasticlib.node.manager.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages replications between repositories.
 */
public class ReplicationsService {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationsService.class);

    private final StorageManager storageManager;
    private final MessageManager messageManager;
    private final ReplicationsDao replicationsDao;
    private final LocalRepositoriesPool localRepositoriesPool;
    private final ReplicationAgentsPool replicationAgentsPool;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param messageManager Messaging infrastructure manager.
     * @param replicationsDao Replications definitions DAO.
     * @param localRepositoriesPool Local repositories pool.
     * @param replicationAgentsPool Replication agents pool.
     */
    public ReplicationsService(StorageManager storageManager,
                               MessageManager messageManager,
                               ReplicationsDao replicationsDao,
                               LocalRepositoriesPool localRepositoriesPool,
                               ReplicationAgentsPool replicationAgentsPool) {
        this.storageManager = storageManager;
        this.messageManager = messageManager;
        this.replicationsDao = replicationsDao;
        this.localRepositoriesPool = localRepositoriesPool;
        this.replicationAgentsPool = replicationAgentsPool;
    }

    /**
     * Starts this service.
     */
    public void start() {
        lock.writeLock().lock();
        try {
            replicationAgentsPool.start();
            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefs().forEach(replicationAgentsPool::startAgent);
            });

            messageManager.register(NewRepositoryEvent.class,
                                    newAction("Signaling replication agents", this::signalAgents));

            messageManager.register(RepositoryOpened.class,
                                    newAction("Starting replications", this::startReplications));

            messageManager.register(RepositoryClosed.class,
                                    newAction("Stopping replications", this::stopReplications));

            messageManager.register(RepositoryRemoved.class,
                                    newAction("Deleting replications", this::deleteReplications));

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Properly stops this service.
     */
    public void stop() {
        lock.writeLock().lock();
        try {
            replicationAgentsPool.stop();

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Creates a new replication from source to destination. Fails if such a replication already exists. Immediately
     * starts the replication, if possible.
     *
     * @param source Source repository name or encoded GUID.
     * @param destination Destination repository name or encoded GUID.
     */
    public void createReplication(String source, String destination) {
        LOG.info("Creating replication {}>{}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                Guid srcId = localRepositoriesPool.getRepositoryDef(source).getGuid();
                Guid destId = localRepositoriesPool.getRepositoryDef(destination).getGuid();
                if (srcId.equals(destId)) {
                    throw new SelfReplicationException();
                }
                ReplicationDef def = new ReplicationDef(Guid.random(), srcId, destId);
                replicationsDao.createReplicationDef(def);
                replicationAgentsPool.createAgent(def);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deletes an existing replication. Fails if it does not exist.
     *
     * @param guid Replication GUID.
     */
    public void deleteReplication(Guid guid) {
        LOG.info("Deleting replication {}", guid);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                replicationsDao.deleteReplicationDef(guid);
                replicationAgentsPool.deleteAgent(guid);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Starts an existing replication. Does nothing if it is already started. Fails if it does not exist or if source or
     * destination repositories are not available.
     *
     * @param guid Replication GUID.
     */
    public void startReplication(Guid guid) {
        LOG.info("Starting replication {}", guid);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                ReplicationDef def = replicationsDao.getReplicationDef(guid);
                replicationAgentsPool.startAgent(def);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Stops an existing replication. Does nothing if such a replication is already stopped. Fails if it does not exist.
     *
     * @param guid Replication GUID.
     */
    public void stopReplication(Guid guid) {
        LOG.info("Stopping replication {}", guid);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                replicationAgentsPool.stopAgent(guid);
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
            return storageManager.inTransaction(() -> {
                Map<Guid, RepositoryDef> repositoryDefs = localRepositoriesPool.listRepositoryDefs()
                        .stream()
                        .collect(toMap(RepositoryDef::getGuid, d -> d));

                return replicationsDao.listReplicationDefs()
                        .stream()
                        .map(def -> replicationInfo(def, repositoryDefs))
                        .collect(toList());
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    private ReplicationInfo replicationInfo(ReplicationDef replicationDef, Map<Guid, RepositoryDef> repositoryDefs) {
        Guid id = replicationDef.getGuid();
        Guid srcId = replicationDef.getSource();
        Guid destId = replicationDef.getDestination();
        Optional<AgentInfo> agentInfo = replicationAgentsPool.getAgentInfo(id);
        if (agentInfo.isPresent()) {
            return new ReplicationInfo(id,
                                       repositoryDefs.get(srcId),
                                       repositoryDefs.get(destId),
                                       agentInfo.get());
        }
        return new ReplicationInfo(id,
                                   repositoryDefs.get(srcId),
                                   repositoryDefs.get(destId));
    }

    private void signalAgents(Guid repositoryGuid) {
        lock.readLock().lock();
        try {
            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefsFrom(repositoryGuid)
                        .stream()
                        .map(ReplicationDef::getGuid)
                        .forEach(replicationAgentsPool::signalAgent);
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    private void startReplications(Guid repositoryGuid) {
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefs(repositoryGuid)
                        .forEach(replicationAgentsPool::tryStartAgent);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void stopReplications(Guid repositoryGuid) {
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefs(repositoryGuid)
                        .stream()
                        .map(ReplicationDef::getGuid)
                        .forEach(replicationAgentsPool::stopAgent);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteReplications(Guid repositoryGuid) {
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefs(repositoryGuid)
                        .stream()
                        .map(ReplicationDef::getGuid)
                        .forEach(guid -> {
                            replicationAgentsPool.deleteAgent(guid);
                            replicationsDao.deleteReplicationDef(guid);
                        });
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static <T extends RepositoryChangeMessage> Action<T> newAction(String description, Consumer<Guid> action) {
        return new Action<T>() {
            @Override
            public String description() {
                return description;
            }

            @Override
            public void apply(T message) {
                action.accept(message.getRepositoryGuid());
            }
        };
    }
}
