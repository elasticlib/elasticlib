package store.server.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;

/**
 * Manages replication agents between repositories.
 */
class ReplicationService {

    private static final String REPLICATION_CURSORS = "replicationCursors";
    private final Database replicationCursors;
    private final Map<String, Map<String, ReplicationAgent>> agents = new HashMap<>();

    public ReplicationService(StorageManager storageManager) {
        replicationCursors = storageManager.openDeferredWriteDatabase(REPLICATION_CURSORS);
    }

    /**
     * Stops all replications.
     */
    public synchronized void close() {
        for (Map<String, ReplicationAgent> map : agents.values()) {
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
     * @param source Source repository name.
     * @param destination Destination repository name.
     */
    public synchronized void startReplication(Repository source, Repository destination) {
        beginReplication(source, destination, false);
    }

    private void beginReplication(Repository source, Repository destination, boolean resetCursor) {
        String srcName = source.getName();
        String destName = destination.getName();
        if (agents.containsKey(srcName) && agents.get(srcName).containsKey(destName)) {
            return;
        }
        if (!agents.containsKey(srcName)) {
            agents.put(srcName, new HashMap<String, ReplicationAgent>());
        }
        DatabaseEntry cursorKey = entry(srcName, destName);
        if (resetCursor) {
            // Ensures agent won't see a stale value.
            replicationCursors.delete(null, cursorKey);
        }
        ReplicationAgent agent = new ReplicationAgent(source, destination, replicationCursors, cursorKey);
        agents.get(srcName).put(destName, agent);
        agent.start();
    }

    /**
     * Stops an existing replication from source to destination. Does nothing if such a replication do not exist.
     *
     * @param source Source repository name.
     * @param destination Destination repository name.
     */
    public synchronized void stopReplication(String source, String destination) {
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
     * @param source Source repository name.
     * @param destination Destination repository name.
     */
    public synchronized void dropReplication(String source, String destination) {
        stopReplication(source, destination);

        // Can't use a transaction on a deffered write database :(
        replicationCursors.delete(null, entry(source, destination));
    }

    /**
     * Stops all replications from/to repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void stopReplications(String name) {
        for (String destination : destinations(name)) {
            stopReplication(name, destination);
        }
        for (String source : sources(name)) {
            stopReplication(source, name);
        }
    }

    /**
     * Drops all replications from/to repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void dropReplications(String name) {
        for (String destination : destinations(name)) {
            dropReplication(name, destination);
        }
        for (String source : sources(name)) {
            dropReplication(source, name);
        }
    }

    private Set<String> destinations(String source) {
        if (!agents.containsKey(source)) {
            return Collections.emptySet();
        }
        // Makes a copy to avoid any weird behaviour in for-loop.
        return new HashSet<>(agents.get(source).keySet());
    }

    private Set<String> sources(String destination) {
        Set<String> sources = new HashSet<>();
        for (Entry<String, Map<String, ReplicationAgent>> entry : agents.entrySet()) {
            if (entry.getValue().keySet().contains(destination)) {
                sources.add(destination);
            }
        }
        return sources;
    }

    /**
     * Signals all agents from repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void signal(String name) {
        if (!agents.containsKey(name)) {
            return;
        }
        for (ReplicationAgent agent : agents.get(name).values()) {
            agent.signal();
        }
    }
}
