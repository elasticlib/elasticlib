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

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import org.elasticlib.common.exception.RepositoryClosedException;
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
import org.elasticlib.node.manager.message.RepositoryClosed;
import org.elasticlib.node.manager.message.RepositoryOpened;
import org.elasticlib.node.manager.message.RepositoryRemoved;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.repository.Agent;
import org.elasticlib.node.repository.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages replication between repositories.
 */
public class ReplicationsService {

    private static final String REPLICATION_CUR_SEQS = "replicationCurSeqs";
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationsService.class);

    private final StorageManager storageManager;
    private final MessageManager messageManager;
    private final ReplicationsDao replicationsDao;
    private final LocalRepositoriesPool localRepositoriesPool;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Guid, Agent> agents = new HashMap<>();
    private Database curSeqsDb;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param messageManager Messaging infrastructure manager.
     * @param replicationsDao Replications definitions DAO.
     * @param localRepositoriesPool Local repositories pool.
     */
    public ReplicationsService(StorageManager storageManager,
                               MessageManager messageManager,
                               ReplicationsDao replicationsDao,
                               LocalRepositoriesPool localRepositoriesPool) {
        this.storageManager = storageManager;
        this.messageManager = messageManager;
        this.replicationsDao = replicationsDao;
        this.localRepositoriesPool = localRepositoriesPool;
    }

    /**
     * Starts this service.
     */
    public void start() {
        lock.writeLock().lock();
        try {
            curSeqsDb = storageManager.openDeferredWriteDatabase(REPLICATION_CUR_SEQS);

            messageManager.register(NewRepositoryEvent.class, new SignalAgentsAction());
            messageManager.register(RepositoryOpened.class, new StartReplicationsAction());
            messageManager.register(RepositoryClosed.class, new StopReplicationsAction());
            messageManager.register(RepositoryRemoved.class, new DeleteReplicationsAction());

            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefs().forEach(this::startAgent);
            });
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
            agents.values()
                    .stream()
                    .forEach(Agent::stop);

            agents.clear();

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Creates a new replication from source to destination. Fails if such a replication already exist.
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
                createAgent(def);
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
                deleteAgent(guid);
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Starts an existing replication. Does nothing if it is already started. Fails if it does not exist or if source or
     * destination repositories are not started.
     *
     * @param guid Replication GUID.
     */
    public void startReplication(Guid guid) {
        LOG.info("Starting replication {}", guid);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                if (!startAgent(replicationsDao.getReplicationDef(guid))) {
                    throw new RepositoryClosedException();
                }
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
                stopAgent(guid);
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
        if (agents.containsKey(id)) {
            AgentInfo agentInfo = agents.get(id).info();
            return new ReplicationInfo(id,
                                       repositoryDefs.get(srcId),
                                       repositoryDefs.get(destId),
                                       agentInfo);
        }
        return new ReplicationInfo(id,
                                   repositoryDefs.get(srcId),
                                   repositoryDefs.get(destId));
    }

    private void createAgent(ReplicationDef def) {
        startAgent(def, true);
    }

    private boolean startAgent(ReplicationDef def) {
        return startAgent(def, false);
    }

    private boolean startAgent(ReplicationDef def, boolean resetCursor) {
        if (agents.containsKey(def.getGuid())) {
            return true;
        }
        Optional<Agent> agent = newAgent(def, resetCursor);
        if (!agent.isPresent()) {
            return false;
        }
        agents.put(def.getGuid(), agent.get());
        agent.get().start();
        return true;
    }

    private Optional<Agent> newAgent(ReplicationDef def, boolean resetCursor) {
        Optional<Repository> source = localRepositoriesPool.tryGetRepository(def.getSource());
        Optional<Repository> destination = localRepositoriesPool.tryGetRepository(def.getDestination());
        if (!source.isPresent() || !destination.isPresent()) {
            return Optional.empty();
        }
        DatabaseEntry curSeqKey = entry(def.getGuid());
        if (resetCursor) {
            // Ensures agent won't see a stale value.
            curSeqsDb.delete(null, curSeqKey);
        }
        return Optional.<Agent>of(new ReplicationAgent(source.get(), destination.get(), curSeqsDb, curSeqKey));
    }

    private void stopAgent(Guid guid) {
        if (!agents.containsKey(guid)) {
            return;
        }
        Agent agent = agents.remove(guid);
        if (agent != null) {
            agent.stop();
        }
    }

    private void deleteAgent(Guid guid) {
        stopAgent(guid);

        // Can't use a transaction on a deffered write database :(
        curSeqsDb.delete(null, entry(guid));
    }

    private class SignalAgentsAction implements Action<NewRepositoryEvent> {

        @Override
        public String description() {
            return "Signaling replication agents";
        }

        @Override
        public void apply(NewRepositoryEvent message) {
            lock.readLock().lock();
            try {
                storageManager.inTransaction(() -> {
                    replicationsDao.listReplicationDefsFrom(message.getRepositoryGuid()).forEach(def -> {
                        agents.get(def.getGuid()).signal();
                    });
                });
            } finally {
                lock.readLock().unlock();
            }
        }
    }

    private class StartReplicationsAction implements Action<RepositoryOpened> {

        @Override
        public String description() {
            return "Starting replications";
        }

        @Override
        public void apply(RepositoryOpened message) {
            lock.writeLock().lock();
            try {
                storageManager.inTransaction(() -> {
                    replicationsDao.listReplicationDefs(message.getRepositoryGuid()).forEach(def -> {
                        startAgent(def);
                    });
                });
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private class StopReplicationsAction implements Action<RepositoryClosed> {

        @Override
        public String description() {
            return "Stopping replications";
        }

        @Override
        public void apply(RepositoryClosed message) {
            lock.writeLock().lock();
            try {
                storageManager.inTransaction(() -> {
                    replicationsDao.listReplicationDefs(message.getRepositoryGuid()).forEach(def -> {
                        stopAgent(def.getGuid());
                    });
                });
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    private class DeleteReplicationsAction implements Action<RepositoryRemoved> {

        @Override
        public String description() {
            return "Deleting replications";
        }

        @Override
        public void apply(RepositoryRemoved message) {
            lock.writeLock().lock();
            try {
                storageManager.inTransaction(() -> {
                    replicationsDao.listReplicationDefs(message.getRepositoryGuid()).forEach(def -> {
                        replicationsDao.deleteReplicationDef(def.getGuid());
                        deleteAgent(def.getGuid());
                    });
                });
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
