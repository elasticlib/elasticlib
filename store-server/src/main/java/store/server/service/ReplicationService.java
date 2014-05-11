package store.server.service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Manage replication agents between repositories.
 */
class ReplicationService {

    private final Map<String, Map<String, ReplicationAgent>> agents = new HashMap<>();

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
     * Create a new replication from source to destination. Does nothing if such a replication already exist.
     *
     * @param source Source repository.
     * @param destination Destination repository.
     */
    public synchronized void createReplication(Repository source, Repository destination) {
        String srcName = source.getName();
        String destName = destination.getName();
        if (agents.containsKey(srcName) && agents.get(srcName).containsKey(destName)) {
            return;
        }
        if (!agents.containsKey(srcName)) {
            agents.put(srcName, new HashMap<String, ReplicationAgent>());
        }
        ReplicationAgent agent = new ReplicationAgent(source, destination);
        agents.get(srcName).put(destName, agent);
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
     * Drop all replications from/to repository which name is supplied.
     *
     * @param name A repository name.
     */
    public synchronized void dropReplications(String name) {
        if (agents.containsKey(name)) {
            for (ReplicationAgent agent : agents.get(name).values()) {
                agent.stop();
            }
            agents.remove(name);
        }
        Iterator<Entry<String, Map<String, ReplicationAgent>>> iterator = agents.entrySet().iterator();
        while (iterator.hasNext()) {
            Map<String, ReplicationAgent> mapping = iterator.next().getValue();
            if (mapping.containsKey(name)) {
                mapping.remove(name).stop();
            }
            if (mapping.isEmpty()) {
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
