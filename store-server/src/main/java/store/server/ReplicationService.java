package store.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Manage replication agents on volumes and indexes within a repository.
 */
public class ReplicationService {

    private final Map<String, Map<String, ReplicationAgent>> agents = new HashMap<>();

    public synchronized void sync(String source, String destination, ReplicationAgent agent) {
        if (!agents.containsKey(source)) {
            agents.put(source, new HashMap<String, ReplicationAgent>());
        }
        if (agents.get(source).containsKey(destination)) {
            return;
        }
        agents.get(source).put(destination, agent);
        agent.start();
    }

    public synchronized void unsync(String source, String destination) {
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

    public synchronized void start(String name) {
        for (ReplicationAgent agent : agents(name)) {
            agent.start();
        }
    }

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

    public synchronized void drop(String name) {
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

    public synchronized void signal(String name) {
        if (!agents.containsKey(name)) {
            return;
        }
        for (ReplicationAgent agent : agents.get(name).values()) {
            agent.signal();
        }
    }
}
