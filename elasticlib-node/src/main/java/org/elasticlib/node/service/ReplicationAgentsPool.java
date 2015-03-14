/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.ReplicationDef;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.repository.Agent;
import org.elasticlib.node.repository.Repository;

/**
 * Manages replication agents.
 */
public class ReplicationAgentsPool {

    private static final String REPLICATION_CUR_SEQS = "replicationCurSeqs";

    private final StorageManager storageManager;
    private final LocalRepositoriesPool localRepositoriesPool;
    private final Map<Guid, Agent> agents = new HashMap<>();
    private Database curSeqsDb;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param localRepositoriesPool Local repositories pool.
     */
    public ReplicationAgentsPool(StorageManager storageManager, LocalRepositoriesPool localRepositoriesPool) {
        this.storageManager = storageManager;
        this.localRepositoriesPool = localRepositoriesPool;
    }

    /**
     * Initializes the pool.
     */
    public void start() {
        curSeqsDb = storageManager.openDeferredWriteDatabase(REPLICATION_CUR_SEQS);
    }

    /**
     * Stops all agents.
     */
    public void stop() {
        agents.values().forEach(Agent::stop);
        agents.clear();
    }

    /**
     * Creates and (if possible) starts a new agent.
     *
     * @param def Definition of the related replication.
     */
    public void createAgent(ReplicationDef def) {
        // Ensures agent won't see a stale value.
        curSeqsDb.delete(null, entry(def.getGuid()));
        tryStartAgent(def);
    }

    /**
     * Starts a new agent. Does nothing if agent is already started.
     *
     * @param def Definition of the related replication.
     */
    public void startAgent(ReplicationDef def) {
        if (agents.containsKey(def.getGuid())) {
            return;
        }
        startAgent(def.getGuid(),
                   localRepositoriesPool.getRepository(def.getSource()),
                   localRepositoriesPool.getRepository(def.getDestination()));
    }

    /**
     * Starts a new agent if its source and destination repositories are available. Does nothing if agent is already
     * started.
     *
     * @param def Definition of the related replication.
     */
    public void tryStartAgent(ReplicationDef def) {
        if (agents.containsKey(def.getGuid())) {
            return;
        }
        Optional<Repository> source = localRepositoriesPool.tryGetRepository(def.getSource());
        Optional<Repository> destination = localRepositoriesPool.tryGetRepository(def.getDestination());
        if (!source.isPresent() || !destination.isPresent()) {
            return;
        }
        startAgent(def.getGuid(), source.get(), destination.get());
    }

    private void startAgent(Guid guid, Repository source, Repository destination) {
        DatabaseEntry curSeqKey = entry(guid);
        Agent agent = new ReplicationAgent(source, destination, curSeqsDb, curSeqKey);

        agents.put(guid, agent);
        agent.start();
    }

    /**
     * Stops an agent. Does nothing if agent is already stopped.
     *
     * @param guid GUID of the related replication.
     */
    public void stopAgent(Guid guid) {
        if (!agents.containsKey(guid)) {
            return;
        }
        agents.remove(guid).stop();
    }

    /**
     * Stops and deletes an agent.
     *
     * @param guid GUID of the related replication.
     */
    public void deleteAgent(Guid guid) {
        stopAgent(guid);

        // Can't use a transaction on a deffered write database :(
        curSeqsDb.delete(null, entry(guid));
    }

    /**
     * Signals an agent. Does nothing if agent is not started.
     *
     * @param guid GUID of the related replication.
     */
    public void signalAgent(Guid guid) {
        if (!agents.containsKey(guid)) {
            return;
        }
        agents.get(guid).signal();
    }

    /**
     * Provides info about an existing agent.
     *
     * @param guid GUID of the related replication.
     * @return Associated agent info, if any.
     */
    public Optional<AgentInfo> getAgentInfo(Guid guid) {
        if (!agents.containsKey(guid)) {
            return Optional.empty();
        }
        return Optional.of(agents.get(guid).info());
    }
}
