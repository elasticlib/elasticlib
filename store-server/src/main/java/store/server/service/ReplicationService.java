package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import store.common.AgentInfo;
import store.common.hash.Guid;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;

/**
 * Manages replication agents between repositories.
 */
class ReplicationService {

    private static final String REPLICATION_CUR_SEQS = "replicationCurSeqs";
    private final Database curSeqsDb;
    private final Map<Guid, Map<Guid, ReplicationAgent>> agents = new HashMap<>();

    public ReplicationService(StorageManager storageManager) {
        curSeqsDb = storageManager.openDeferredWriteDatabase(REPLICATION_CUR_SEQS);
    }

    /**
     * Stops all replications.
     */
    public synchronized void close() {
        for (Map<Guid, ReplicationAgent> map : agents.values()) {
            for (ReplicationAgent agent : map.values()) {
                agent.stop();
            }
        }
    }

    /**
     * Creates a new replication from source to destination. Does nothing if such a replication already exists and is
     * already started.
     *
     * @param source Source repository.
     * @param destination Destination repository.
     */
    public synchronized void createReplication(Repository source, Repository destination) {
        beginReplication(source, destination, true);
    }

    /**
     * Starts an existing replication from source to destination. Does nothing if such a replication is already started.
     *
     * @param source Source repository.
     * @param destination Destination repository.
     */
    public synchronized void startReplication(Repository source, Repository destination) {
        beginReplication(source, destination, false);
    }

    private void beginReplication(Repository source, Repository destination, boolean resetCursor) {
        Guid srcId = source.getDef().getGuid();
        Guid destId = destination.getDef().getGuid();
        if (agents.containsKey(srcId) && agents.get(srcId).containsKey(destId)) {
            return;
        }
        if (!agents.containsKey(srcId)) {
            agents.put(srcId, new HashMap<Guid, ReplicationAgent>());
        }
        DatabaseEntry curSeqKey = entry(srcId, destId);
        if (resetCursor) {
            // Ensures agent won't see a stale value.
            curSeqsDb.delete(null, curSeqKey);
        }
        ReplicationAgent agent = new ReplicationAgent(source, destination, curSeqsDb, curSeqKey);
        agents.get(srcId).put(destId, agent);
        agent.start();
    }

    /**
     * Stops an existing replication from source to destination. Does nothing if such a replication do not exist.
     *
     * @param source Source repository GUID.
     * @param destination Destination repository GUID.
     */
    public synchronized void stopReplication(Guid source, Guid destination) {
        if (!agents.containsKey(source)) {
            return;
        }
        ReplicationAgent agent = agents.get(source).remove(destination);
        if (agent != null) {
            agent.stop();
        }
        if (agents.get(source).isEmpty()) {
            agents.remove(source);
        }
    }

    /**
     * Stops an existing replication from source to destination and delete related persisted data. Does nothing if such
     * a replication do not exist.
     *
     * @param source Source repository GUID.
     * @param destination Destination repository GUID.
     */
    public synchronized void deleteReplication(Guid source, Guid destination) {
        stopReplication(source, destination);

        // Can't use a transaction on a deffered write database :(
        curSeqsDb.delete(null, entry(source, destination));
    }

    /**
     * Stops all replications from/to repository which GUID is supplied.
     *
     * @param guid A repository GUID.
     */
    public synchronized void stopReplications(Guid guid) {
        for (Guid destination : destinations(guid)) {
            stopReplication(guid, destination);
        }
        for (Guid source : sources(guid)) {
            stopReplication(source, guid);
        }
    }

    /**
     * Drops all replications from/to repository which GUID is supplied.
     *
     * @param guid A repository GUID.
     */
    public synchronized void dropReplications(Guid guid) {
        for (Guid destination : destinations(guid)) {
            deleteReplication(guid, destination);
        }
        for (Guid source : sources(guid)) {
            deleteReplication(source, guid);
        }
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
        for (Entry<Guid, Map<Guid, ReplicationAgent>> entry : agents.entrySet()) {
            if (entry.getValue().keySet().contains(destination)) {
                sources.add(entry.getKey());
            }
        }
        return sources;
    }

    /**
     * Signals all agents from repository which GUID is supplied.
     *
     * @param guid A repository GUID.
     */
    public synchronized void signal(Guid guid) {
        if (!agents.containsKey(guid)) {
            return;
        }
        for (ReplicationAgent agent : agents.get(guid).values()) {
            agent.signal();
        }
    }

    /**
     * Provides agent info of the replication from source to destination, if such a replication exists and is started.
     *
     * @param source Source repository GUID.
     * @param destination Destination repository GUID.
     * @return Corresponding agent info, if this replication is started.
     */
    public synchronized Optional<AgentInfo> getInfo(Guid source, Guid destination) {
        if (!agents.containsKey(source) || !agents.get(source).containsKey(destination)) {
            return Optional.absent();
        }
        return Optional.of(agents.get(source).get(destination).info());
    }
}
