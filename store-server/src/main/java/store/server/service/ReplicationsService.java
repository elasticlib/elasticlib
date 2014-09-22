package store.server.service;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.AgentInfo;
import store.common.ReplicationDef;
import store.common.ReplicationInfo;
import store.common.RepositoryDef;
import store.common.hash.Guid;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.exception.RepositoryClosedException;
import store.server.exception.SelfReplicationException;
import store.server.manager.message.Action;
import store.server.manager.message.MessageManager;
import store.server.manager.message.NewRepositoryEvent;
import store.server.manager.message.RepositoryClosed;
import store.server.manager.message.RepositoryOpened;
import store.server.manager.message.RepositoryRemoved;
import static store.server.manager.storage.DatabaseEntries.entry;
import store.server.manager.storage.Procedure;
import store.server.manager.storage.Query;
import store.server.manager.storage.StorageManager;
import store.server.repository.Agent;
import store.server.repository.Repository;

/**
 * Manages replication between repositories.
 */
public class ReplicationsService {

    private static final String REPLICATION_CUR_SEQS = "replicationCurSeqs";
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationsService.class);
    private final StorageManager storageManager;
    private final MessageManager messageManager;
    private final RepositoriesDao repositoriesDao;
    private final ReplicationsDao replicationsDao;
    private final RepositoriesService repositoriesService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Guid, Map<Guid, Agent>> agents = new HashMap<>();
    private Database curSeqsDb;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param messageManager Messaging infrastructure manager.
     * @param repositoriesDao Repositories definitions DAO.
     * @param replicationsDao Replications definitions DAO.
     * @param repositoriesService Repositories Service.
     */
    public ReplicationsService(StorageManager storageManager,
                               MessageManager messageManager,
                               RepositoriesDao repositoriesDao,
                               ReplicationsDao replicationsDao,
                               RepositoriesService repositoriesService) {
        this.storageManager = storageManager;
        this.messageManager = messageManager;
        this.repositoriesDao = repositoriesDao;
        this.replicationsDao = replicationsDao;
        this.repositoriesService = repositoriesService;
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

            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    for (ReplicationDef def : replicationsDao.listReplicationDefs()) {
                        startReplication(def);
                    }
                }
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
            for (Map<Guid, Agent> map : agents.values()) {
                for (Agent agent : map.values()) {
                    agent.stop();
                }
            }
            agents.clear();

        } finally {
            lock.writeLock().unlock();
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
                    Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                    Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                    ReplicationDef def = new ReplicationDef(srcId, destId);
                    if (replicationsDao.createReplicationDef(def)) {
                        createReplication(def);
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
        LOG.info("Deleting replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                    Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                    if (replicationsDao.deleteReplicationDef(srcId, destId)) {
                        stopReplication(source, destination);
                        curSeqsDb.delete(null, entry(srcId, destId));
                    }
                }
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Start an existing replication from source to destination. Does nothing if replication is already started. Fails
     * if source or destination are not started.
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
                    Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                    Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                    if (!startReplication(replicationsDao.getReplicationDef(srcId, destId))) {
                        throw new RepositoryClosedException();
                    }
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
                    Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                    Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                    stopReplication(srcId, destId);
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
                    Map<Guid, RepositoryDef> repositoryDefs = repositoryDefs();
                    for (ReplicationDef replicationDef : replicationsDao.listReplicationDefs()) {
                        list.add(replicationInfo(replicationDef, repositoryDefs));
                    }
                    return list;
                }
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    private Map<Guid, RepositoryDef> repositoryDefs() {
        return Maps.uniqueIndex(repositoriesDao.listRepositoryDefs(), new Function<RepositoryDef, Guid>() {
            @Override
            public Guid apply(RepositoryDef def) {
                return def.getGuid();
            }
        });
    }

    private ReplicationInfo replicationInfo(ReplicationDef replicationDef, Map<Guid, RepositoryDef> repositoryDefs) {
        Guid srcId = replicationDef.getSource();
        Guid destId = replicationDef.getDestination();
        if (agents.containsKey(srcId) && agents.get(srcId).containsKey(destId)) {
            AgentInfo agentInfo = agents.get(srcId).get(destId).info();
            return new ReplicationInfo(repositoryDefs.get(srcId), repositoryDefs.get(destId), agentInfo);
        }
        return new ReplicationInfo(repositoryDefs.get(srcId), repositoryDefs.get(destId));
    }

    private void createReplication(ReplicationDef def) {
        startReplication(def, true);
    }

    private boolean startReplication(ReplicationDef def) {
        return startReplication(def, false);
    }

    private boolean startReplication(ReplicationDef def, boolean resetCursor) {
        Guid srcId = def.getSource();
        Guid destId = def.getDestination();
        if (agents.containsKey(srcId) && agents.get(srcId).containsKey(destId)) {
            return true;
        }
        Optional<Agent> agent = newAgent(srcId, destId, resetCursor);
        if (!agent.isPresent()) {
            return false;
        }
        if (!agents.containsKey(srcId)) {
            agents.put(srcId, new HashMap<Guid, Agent>());
        }
        agents.get(srcId).put(destId, agent.get());
        agent.get().start();
        return true;
    }

    private Optional<Agent> newAgent(Guid srcId, Guid destId, boolean resetCursor) {
        Optional<Repository> source = repositoriesService.tryGetRepository(srcId);
        Optional<Repository> destination = repositoriesService.tryGetRepository(destId);
        if (!source.isPresent() || !destination.isPresent()) {
            return Optional.absent();
        }
        DatabaseEntry curSeqKey = entry(srcId, destId);
        if (resetCursor) {
            // Ensures agent won't see a stale value.
            curSeqsDb.delete(null, curSeqKey);
        }
        return Optional.<Agent>of(new ReplicationAgent(source.get(), destination.get(), curSeqsDb, curSeqKey));
    }

    private void stopReplication(Guid source, Guid destination) {
        if (!agents.containsKey(source)) {
            return;
        }
        Agent agent = agents.get(source).remove(destination);
        if (agent != null) {
            agent.stop();
        }
        if (agents.get(source).isEmpty()) {
            agents.remove(source);
        }
    }

    private void deleteReplication(Guid source, Guid destination) {
        stopReplication(source, destination);

        // Can't use a transaction on a deffered write database :(
        curSeqsDb.delete(null, entry(source, destination));
    }

    private Set<Guid> destinations(Guid source) {
        if (!agents.containsKey(source)) {
            return Collections.emptySet();
        }
        // Makes a copy to avoid any weird behaviour in for-loop.
        return new HashSet<>(agents.get(source).keySet());
    }

    private Set<Guid> sources(Guid destination) {
        Set<Guid> sources = new HashSet<>();
        for (Map.Entry<Guid, Map<Guid, Agent>> entry : agents.entrySet()) {
            if (entry.getValue().keySet().contains(destination)) {
                sources.add(entry.getKey());
            }
        }
        return sources;
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
                Guid guid = message.getRepositoryGuid();
                if (!agents.containsKey(guid)) {
                    return;
                }
                for (Agent agent : agents.get(guid).values()) {
                    agent.signal();
                }
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
                final Guid guid = message.getRepositoryGuid();
                storageManager.inTransaction(new Procedure() {
                    @Override
                    public void apply() {
                        for (ReplicationDef def : replicationsDao.listReplicationDefs(guid)) {
                            startReplication(def);
                        }
                    }
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
                Guid guid = message.getRepositoryGuid();
                for (Guid destination : destinations(guid)) {
                    stopReplication(guid, destination);
                }
                for (Guid source : sources(guid)) {
                    stopReplication(source, guid);
                }
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
                final Guid guid = message.getRepositoryGuid();
                storageManager.inTransaction(new Procedure() {
                    @Override
                    public void apply() {
                        replicationsDao.deleteAllReplicationDefs(guid);
                    }
                });
                for (Guid destination : destinations(guid)) {
                    deleteReplication(guid, destination);
                }
                for (Guid source : sources(guid)) {
                    deleteReplication(source, guid);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
