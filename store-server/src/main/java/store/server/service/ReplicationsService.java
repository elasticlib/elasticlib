package store.server.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.exception.RepositoryClosedException;
import store.common.exception.SelfReplicationException;
import store.common.hash.Guid;
import store.common.model.AgentInfo;
import store.common.model.ReplicationDef;
import store.common.model.ReplicationInfo;
import store.common.model.RepositoryDef;
import store.server.dao.ReplicationsDao;
import store.server.dao.RepositoriesDao;
import store.server.manager.message.Action;
import store.server.manager.message.MessageManager;
import store.server.manager.message.NewRepositoryEvent;
import store.server.manager.message.RepositoryClosed;
import store.server.manager.message.RepositoryOpened;
import store.server.manager.message.RepositoryRemoved;
import static store.server.manager.storage.DatabaseEntries.entry;
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

            storageManager.inTransaction(() -> {
                replicationsDao.listReplicationDefs()
                        .stream()
                        .forEach(this::startReplication);
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
                    .flatMap(map -> map.values().stream())
                    .forEach(Agent::stop);

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
    public void createReplication(String source, String destination) {
        LOG.info("Creating replication {}>{}", source, destination);
        if (source.equals(destination)) {
            throw new SelfReplicationException();
        }
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                ReplicationDef def = new ReplicationDef(srcId, destId);
                if (replicationsDao.createReplicationDef(def)) {
                    createReplication(def);
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
    public void deleteReplication(String source, String destination) {
        LOG.info("Deleting replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                if (replicationsDao.deleteReplicationDef(srcId, destId)) {
                    stopReplication(source, destination);
                    curSeqsDb.delete(null, entry(srcId, destId));
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
    public void startReplication(String source, String destination) {
        LOG.info("Starting replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                if (!startReplication(replicationsDao.getReplicationDef(srcId, destId))) {
                    throw new RepositoryClosedException();
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
    public void stopReplication(String source, String destination) {
        LOG.info("Stopping replication {} >> {}", source, destination);
        lock.writeLock().lock();
        try {
            storageManager.inTransaction(() -> {
                Guid srcId = repositoriesDao.getRepositoryDef(source).getGuid();
                Guid destId = repositoriesDao.getRepositoryDef(destination).getGuid();

                stopReplication(srcId, destId);
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
                Map<Guid, RepositoryDef> repositoryDefs = repositoriesDao.listRepositoryDefs()
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
            agents.put(srcId, new HashMap<>());
        }
        agents.get(srcId).put(destId, agent.get());
        agent.get().start();
        return true;
    }

    private Optional<Agent> newAgent(Guid srcId, Guid destId, boolean resetCursor) {
        Optional<Repository> source = repositoriesService.tryGetRepository(srcId);
        Optional<Repository> destination = repositoriesService.tryGetRepository(destId);
        if (!source.isPresent() || !destination.isPresent()) {
            return Optional.empty();
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
        return agents.entrySet()
                .stream()
                .filter(e -> e.getValue().keySet().contains(destination))
                .map(Entry::getKey)
                .collect(toSet());
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
                agents.get(guid)
                        .values()
                        .stream()
                        .forEach(Agent::signal);
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
                Guid guid = message.getRepositoryGuid();
                storageManager.inTransaction(() -> {
                    replicationsDao.listReplicationDefs(guid)
                            .stream()
                            .forEach(def -> startReplication(def));
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
                destinations(guid).forEach(destination -> stopReplication(guid, destination));
                sources(guid).forEach(source -> stopReplication(source, guid));

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
                Guid guid = message.getRepositoryGuid();
                storageManager.inTransaction(() -> {
                    replicationsDao.deleteAllReplicationDefs(guid);
                });
                destinations(guid).forEach(destination -> deleteReplication(guid, destination));
                sources(guid).forEach(source -> deleteReplication(source, guid));

            } finally {
                lock.writeLock().unlock();
            }
        }
    }
}
