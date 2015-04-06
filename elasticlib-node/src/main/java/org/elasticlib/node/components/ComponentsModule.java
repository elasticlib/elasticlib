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

import org.elasticlib.common.config.Config;
import org.elasticlib.node.dao.AttributesDao;
import org.elasticlib.node.dao.CurSeqsDao;
import org.elasticlib.node.dao.DaoModule;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.manager.client.ClientManager;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.manager.task.TaskManager;

/**
 * Provides service components.
 */
public class ComponentsModule {

    private final StorageManager storageManager;
    private final NodeNameProvider nodeNameProvider;
    private final NodeGuidProvider nodeGuidProvider;
    private final PublishUrisProvider publishUrisProvider;
    private final NodePingHandler nodePingHandler;
    private final LocalRepositoriesPool localRepositoriesPool;
    private final RemoteRepositoriesPool remoteRepositoriesPool;
    private final RepositoriesProvider repositoriesProvider;
    private final ReplicationAgentsPool replicationAgentsPool;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param managerModule Manager module.
     * @param daoModule DAO module.
     */
    public ComponentsModule(Config config, ManagerModule managerModule, DaoModule daoModule) {
        storageManager = managerModule.getStorageManager();
        ClientManager clientManager = managerModule.getClientManager();
        TaskManager taskManager = managerModule.getTaskManager();
        MessageManager messageManager = managerModule.getMessageManager();

        AttributesDao attributesDao = daoModule.getAttributesDao();
        RemotesDao remotesDao = daoModule.getRemotesDao();
        RepositoriesDao repositoriesDao = daoModule.getRepositoriesDao();
        CurSeqsDao curSeqsDao = daoModule.getCurSeqsDao();

        nodeNameProvider = new NodeNameProvider(config);
        nodeGuidProvider = new NodeGuidProvider(attributesDao);
        publishUrisProvider = new PublishUrisProvider(config);
        nodePingHandler = new NodePingHandler(clientManager);

        LocalRepositoriesFactory localRepositoriesFactory = new LocalRepositoriesFactory(config,
                                                                                         taskManager,
                                                                                         messageManager);

        localRepositoriesPool = new LocalRepositoriesPool(repositoriesDao, localRepositoriesFactory);
        remoteRepositoriesPool = new RemoteRepositoriesPool(clientManager, messageManager, remotesDao);
        repositoriesProvider = new RepositoriesProvider(localRepositoriesPool, remoteRepositoriesPool);
        replicationAgentsPool = new ReplicationAgentsPool(config, curSeqsDao, repositoriesProvider);
    }

    /**
     * Starts the module.
     */
    public void start() {
        storageManager.inTransaction(nodeGuidProvider::start);
        storageManager.inTransaction(localRepositoriesPool::start);
        remoteRepositoriesPool.start();
        replicationAgentsPool.start();
    }

    /**
     * Stops the module.
     */
    public void stop() {
        replicationAgentsPool.stop();
        remoteRepositoriesPool.stop();
        localRepositoriesPool.stop();
        nodeGuidProvider.stop();
    }

    /**
     * @return The local node name provider.
     */
    public NodeNameProvider getNodeNameProvider() {
        return nodeNameProvider;
    }

    /**
     * @return The local node GUID provider.
     */
    public NodeGuidProvider getNodeGuidProvider() {
        return nodeGuidProvider;
    }

    /**
     * @return The local node publish URI(s) provider.
     */
    public PublishUrisProvider getPublishUrisProvider() {
        return publishUrisProvider;
    }

    /**
     * @return The remote nodes ping handler.
     */
    public NodePingHandler getNodePingHandler() {
        return nodePingHandler;
    }

    /**
     * @return The local repositories pool.
     */
    public LocalRepositoriesPool getLocalRepositoriesPool() {
        return localRepositoriesPool;
    }

    /**
     * @return The remote repositories pool.
     */
    public RemoteRepositoriesPool getRemoteRepositoriesPool() {
        return remoteRepositoriesPool;
    }

    /**
     * @return The repositories provider.
     */
    public RepositoriesProvider getRepositoriesProvider() {
        return repositoriesProvider;
    }

    /**
     * @return The replication agents pool.
     */
    public ReplicationAgentsPool getReplicationAgentsPool() {
        return replicationAgentsPool;
    }
}
