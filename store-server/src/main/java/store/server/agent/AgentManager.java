package store.server.agent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import store.common.Uid;
import store.server.Index;
import store.server.volume.Volume;

/**
 * Manage replication agents on volumes and indexes within a repository.
 */
public class AgentManager {

    private final Map<Uid, Map<Uid, Agent>> agents = new HashMap<>();

    public synchronized void sync(Uid sourceId, Volume source, Uid destinationId, Volume destination) {
        sync(sourceId, destinationId, new SyncAgent(this, destinationId, source, destination));
    }

    public synchronized void sync(Uid sourceId, Volume source, Uid destinationId, Index destination) {
        sync(sourceId, destinationId, new IndexAgent(source, destination));
    }

    private void sync(Uid sourceId, Uid destinationId, Agent agent) {
        if (!agents.containsKey(sourceId)) {
            agents.put(sourceId, new HashMap<Uid, Agent>());
        }
        if (agents.get(sourceId).containsKey(destinationId)) {
            return;
        }
        agents.get(sourceId).put(destinationId, agent);
        agent.start();
    }

    public synchronized void unsync(Uid sourceId, Uid destinationId) {
        if (!agents.containsKey(sourceId)) {
            return;
        }
        Agent agent = agents.get(sourceId).remove(destinationId);
        if (agent != null) {
            agent.stop();
        }
        if (agents.get(sourceId).isEmpty()) {
            agents.remove(sourceId);
        }
    }

    public synchronized void start(Uid uid) {
        for (Agent agent : agents(uid)) {
            agent.start();
        }
    }

    public synchronized void stop(Uid uid) {
        for (Agent agent : agents(uid)) {
            agent.stop();
        }
    }

    private Collection<Agent> agents(Uid uid) {
        Collection<Agent> collection = new ArrayList<>();
        if (agents.containsKey(uid)) {
            collection.addAll(agents.get(uid).values());
        }
        for (Map<Uid, Agent> map : agents.values()) {
            if (map.containsKey(uid)) {
                collection.add(map.get(uid));
            }
        }
        return collection;
    }

    public synchronized void drop(Uid uid) {
        stop(uid);
        agents.remove(uid);
        Iterator<Entry<Uid, Map<Uid, Agent>>> iterator = agents.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<Uid, Map<Uid, Agent>> entry = iterator.next();
            entry.getValue().remove(uid);
            if (entry.getValue().isEmpty()) {
                iterator.remove();
            }
        }
    }

    public synchronized void signal(Uid uid) {
        if (!agents.containsKey(uid)) {
            return;
        }
        for (Agent agent : agents.get(uid).values()) {
            agent.signal();
        }
    }
}
