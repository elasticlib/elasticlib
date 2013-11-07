package store.server.agent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import store.server.Index;
import store.server.volume.Volume;

/**
 * Manage replication agents on volumes and indexes within a repository.
 */
public class AgentManager {

    private final Map<String, Map<String, Agent>> agents = new HashMap<>();

    public synchronized void sync(Volume source, Volume destination) {
        sync(source.getName(), destination.getName(), new SyncAgent(this, source, destination));
    }

    public synchronized void sync(Volume source, Index destination) {
        sync(source.getName(), destination.getName(), new IndexAgent(source, destination));
    }

    private void sync(String source, String destination, Agent agent) {
        if (!agents.containsKey(source)) {
            agents.put(source, new HashMap<String, Agent>());
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
        Agent agent = agents.get(source).remove(destination);
        if (agent != null) {
            agent.stop();
        }
        if (agents.get(source).isEmpty()) {
            agents.remove(source);
        }
    }

    public synchronized void start(String name) {
        for (Agent agent : agents(name)) {
            agent.start();
        }
    }

    public synchronized void stop(String name) {
        for (Agent agent : agents(name)) {
            agent.stop();
        }
    }

    private Collection<Agent> agents(String name) {
        Collection<Agent> collection = new ArrayList<>();
        if (agents.containsKey(name)) {
            collection.addAll(agents.get(name).values());
        }
        for (Map<String, Agent> map : agents.values()) {
            if (map.containsKey(name)) {
                collection.add(map.get(name));
            }
        }
        return collection;
    }

    public synchronized void drop(String name) {
        stop(name);
        agents.remove(name);
        Iterator<Entry<String, Map<String, Agent>>> iterator = agents.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Map<String, Agent>> entry = iterator.next();
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
        for (Agent agent : agents.get(name).values()) {
            agent.signal();
        }
    }
}
