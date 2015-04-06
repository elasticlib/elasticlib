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
package org.elasticlib.node.components;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.AgentInfo;
import org.elasticlib.common.model.AgentState;
import org.elasticlib.common.model.ReplicationDef;
import org.elasticlib.node.dao.CurSeqsDao;
import org.elasticlib.node.repository.Agent;
import org.elasticlib.node.repository.Repository;

/**
 * Manages replication agents.
 */
public class ReplicationAgentsPool {

    private final CurSeqsDao curSeqsDao;
    private final RepositoriesProvider repositoriesProvider;
    private final Map<Guid, Agent> agents = new HashMap<>();

    /**
     * Constructor.
     *
     * @param curSeqsDao The agents sequences DAO.
     * @param repositoriesProvider Repositories provider.
     */
    public ReplicationAgentsPool(CurSeqsDao curSeqsDao, RepositoriesProvider repositoriesProvider) {
        this.curSeqsDao = curSeqsDao;
        this.repositoriesProvider = repositoriesProvider;
    }

    /**
     * Initializes the pool.
     */
    public void start() {
        // Nothing to do.
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
        curSeqsDao.delete(def.getGuid().asHexadecimalString());
        tryStartAgent(def);
    }

    /**
     * Starts a new agent. Does nothing if agent is already started.
     *
     * @param def Definition of the related replication.
     */
    public void startAgent(ReplicationDef def) {
        if (isStarted(def.getGuid())) {
            return;
        }
        startAgent(def.getGuid(),
                   repositoriesProvider.getRepository(def.getSource()),
                   repositoriesProvider.getRepository(def.getDestination()));
    }

    /**
     * Starts a new agent if its source and destination repositories are available. Does nothing if agent is already
     * started.
     *
     * @param def Definition of the related replication.
     */
    public void tryStartAgent(ReplicationDef def) {
        if (isStarted(def.getGuid())) {
            return;
        }
        Optional<Repository> source = repositoriesProvider.tryGetRepository(def.getSource());
        Optional<Repository> destination = repositoriesProvider.tryGetRepository(def.getDestination());
        if (!source.isPresent() || !destination.isPresent()) {
            return;
        }
        startAgent(def.getGuid(), source.get(), destination.get());
    }

    private boolean isStarted(Guid guid) {
        if (!agents.containsKey(guid)) {
            return false;
        }
        AgentState state = agents.get(guid).info().getState();
        return state != AgentState.STOPPED && state != AgentState.ERROR;
    }

    private void startAgent(Guid guid, Repository source, Repository destination) {
        Agent agent = new ReplicationAgent(guid, source, destination, curSeqsDao, guid.asHexadecimalString());
        Agent previous = agents.put(guid, agent);
        if (previous != null) {
            previous.stop();
        }
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
        curSeqsDao.delete(guid.asHexadecimalString());
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
     * Provides info about an existing agent, if it is started.
     *
     * @param guid GUID of the related replication.
     * @return Associated agent info, if any.
     */
    public Optional<AgentInfo> tryGetAgentInfo(Guid guid) {
        if (!agents.containsKey(guid)) {
            return Optional.empty();
        }
        return Optional.of(agents.get(guid).info());
    }
}
