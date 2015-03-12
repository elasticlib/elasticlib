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

import org.elasticlib.common.config.Config;
import org.elasticlib.node.dao.AttributesDao;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.dao.ReplicationsDao;
import org.elasticlib.node.dao.RepositoriesDao;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.manager.message.MessageManager;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.manager.task.TaskManager;

/**
 * Manages services life-cycle.
 */
public class ServiceModule {

    private final RepositoriesService repositoriesService;
    private final ReplicationsService replicationsService;
    private final NodeService nodeService;
    private final RemotesService remotesService;

    /**
     * Constructor. Build and starts all services.
     *
     * @param config Configuration holder.
     * @param managerModule Manager module.
     */
    public ServiceModule(Config config, ManagerModule managerModule) {
        TaskManager taskManager = managerModule.getTaskManager();
        StorageManager storageManager = managerModule.getStorageManager();
        MessageManager messageManager = managerModule.getMessageManager();

        AttributesDao attributesDao = new AttributesDao(storageManager);
        RemotesDao remotesDao = new RemotesDao(storageManager);
        RepositoriesDao repositoriesDao = new RepositoriesDao(storageManager);
        ReplicationsDao replicationsDao = new ReplicationsDao(storageManager);

        LocalRepositoriesFactory localRepositoriesFactory = new LocalRepositoriesFactory(config,
                                                                                         taskManager,
                                                                                         messageManager);
        LocalRepositoriesPool localRepositoriesPool = new LocalRepositoriesPool(repositoriesDao,
                                                                                localRepositoriesFactory);
        NodeNameProvider nodeNameProvider = new NodeNameProvider(config);
        PublishUrisProvider publishUrisProvider = new PublishUrisProvider(config);
        NodePingHandler nodePingHandler = new NodePingHandler();

        repositoriesService = new RepositoriesService(storageManager,
                                                      messageManager,
                                                      localRepositoriesPool);

        replicationsService = new ReplicationsService(storageManager,
                                                      messageManager,
                                                      replicationsDao,
                                                      localRepositoriesPool);

        nodeService = new NodeService(storageManager,
                                      attributesDao,
                                      localRepositoriesPool,
                                      nodeNameProvider,
                                      publishUrisProvider);

        remotesService = new RemotesService(config,
                                            taskManager,
                                            storageManager,
                                            remotesDao,
                                            nodeService,
                                            nodePingHandler);
    }

    /**
     * Starts the module.
     */
    public void start() {
        repositoriesService.start();
        replicationsService.start();
        nodeService.start();
        remotesService.start();
    }

    /**
     * Properly stops all services.
     */
    public void stop() {
        remotesService.stop();
        nodeService.stop();
        replicationsService.stop();
        repositoriesService.stop();
    }

    /**
     * @return The repositories service.
     */
    public RepositoriesService getRepositoriesService() {
        return repositoriesService;
    }

    /**
     * @return The replications service.
     */
    public ReplicationsService getReplicationsService() {
        return replicationsService;
    }

    /**
     * @return The local node service.
     */
    public NodeService getNodeService() {
        return nodeService;
    }

    /**
     * @return The remote nodes service.
     */
    public RemotesService getRemotesService() {
        return remotesService;
    }
}
