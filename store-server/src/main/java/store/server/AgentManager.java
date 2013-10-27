package store.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import store.common.Uid;

public class AgentManager {

    private final Map<Uid, Map<Uid, Agent>> agents = new HashMap<>();

    public synchronized void sync(Uid sourceId, Volume source, Uid destinationId, Volume destination) {
        if (!agents.containsKey(sourceId)) {
            agents.put(sourceId, new HashMap<Uid, Agent>());
        }
        if (agents.get(sourceId).containsKey(destinationId)) {
            return;
        }
        Agent agent = new Agent(this, destinationId, source, destination);
        agents.get(sourceId).put(destinationId, agent);
        agent.signal();
    }

    public synchronized void unsync(Uid sourceId, Uid destinationId) {
        if (!agents.containsKey(sourceId)) {
            return;
        }
        Agent agent = agents.get(sourceId).remove(destinationId);
        if (agent != null) {
            agent.close();
        }
        if (agents.get(sourceId).isEmpty()) {
            agents.remove(sourceId);
        }
    }

    public synchronized void close(Uid uid) {
        if (agents.containsKey(uid)) {
            for (Agent agent : agents.get(uid).values()) {
                agent.close();
            }
            agents.remove(uid);
        }
        List<Uid> toBeRemoved = new ArrayList<>();
        for (Entry<Uid, Map<Uid, Agent>> entry : agents.entrySet()) {
            Agent agent = entry.getValue().remove(uid);
            if (agent != null) {
                agent.close();
            }
            if (entry.getValue().isEmpty()) {
                toBeRemoved.add(entry.getKey());
            }
        }
        for (Uid key : toBeRemoved) {
            agents.remove(key);
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
