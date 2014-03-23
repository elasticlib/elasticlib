package store.server.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Manage replication agents between repositories.
 */
public class ReplicationService {

    private final Map<String, Map<String, ReplicationAgent>> agents = new HashMap<>();

    /**
     * Create a new replication from source to destination. Does nothing if such a replication already exist.
     *
     * @param source Source repository name.
     * @param destination Destination repository name.
     * @param agent Agent used to perform replication.
     */
    public synchronized void createReplication(String source, String destination, ReplicationAgent agent) {
        if (!agents.containsKey(source)) {
            agents.put(source, new HashMap<String, ReplicationAgent>());
        }
        if (agents.get(source).containsKey(destination)) {
            return;
        }
        agents.get(source).put(destination, agent);
        agent.start();
    }

    /**
     * Drop an existing replication from source to destination. Does nothing if such a replication do not exist.
     *
     * @param source Source repository name.
     * @param destination Destination repository name.
     */
    public synchronized void dropReplication(String source, String destination) {
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
     * Start all agents from/to repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void start(String name) {
        for (ReplicationAgent agent : agents(name)) {
            agent.start();
        }
    }

    /**
     * Stop all agents from/to repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void stop(String name) {
        for (ReplicationAgent agent : agents(name)) {
            agent.stop();
        }
    }

    private Collection<ReplicationAgent> agents(String name) {
        Collection<ReplicationAgent> collection = new ArrayList<>();
        if (agents.containsKey(name)) {
            collection.addAll(agents.get(name).values());
        }
        for (Map<String, ReplicationAgent> map : agents.values()) {
            if (map.containsKey(name)) {
                collection.add(map.get(name));
            }
        }
        return collection;
    }

    /**
     * Drop all replications from/to repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void dropReplications(String name) {
        stop(name);
        agents.remove(name);
        Iterator<Entry<String, Map<String, ReplicationAgent>>> iterator = agents.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Map<String, ReplicationAgent>> entry = iterator.next();
            entry.getValue().remove(name);
            if (entry.getValue().isEmpty()) {
                iterator.remove();
            }
        }
    }

    /**
     * Signal all agents from repository which name is supplied.
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
