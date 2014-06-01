package store.server.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import store.common.hash.Guid;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;

/**
 * Manages replication agents between repositories.
 */
class ReplicationService {

    private static final String REPLICATION_CURSORS = "replicationCursors";
    private final Database replicationCursors;
    private final Map<Guid, Map<Guid, ReplicationAgent>> agents = new HashMap<>();

    public ReplicationService(StorageManager storageManager) {
        replicationCursors = storageManager.openDeferredWriteDatabase(REPLICATION_CURSORS);
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
        Guid srcId = source.getGuid();
        Guid destId = destination.getGuid();
        if (agents.containsKey(srcId) && agents.get(srcId).containsKey(destId)) {
            return;
        }
        if (!agents.containsKey(srcId)) {
            agents.put(srcId, new HashMap<Guid, ReplicationAgent>());
        }
        DatabaseEntry cursorKey = entry(srcId, destId);
        if (resetCursor) {
            // Ensures agent won't see a stale value.
            replicationCursors.delete(null, cursorKey);
        }
        ReplicationAgent agent = new ReplicationAgent(source, destination, replicationCursors, cursorKey);
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
        replicationCursors.delete(null, entry(source, destination));
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
                sources.add(destination);
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
}
