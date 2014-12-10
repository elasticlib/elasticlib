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
import org.elasticlib.node.dao.NodesDao;
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
    private final NodesService nodesService;

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
        NodesDao nodesDao = new NodesDao(storageManager);
        RepositoriesDao repositoriesDao = new RepositoriesDao(storageManager);
        ReplicationsDao replicationsDao = new ReplicationsDao(storageManager);

        NodeNameProvider nodeNameProvider = new NodeNameProvider(config);
        PublishUrisProvider publishUrisProvider = new PublishUrisProvider(config);
        NodePingHandler nodePingHandler = new NodePingHandler();

        repositoriesService = new RepositoriesService(config,
                                                      taskManager,
                                                      storageManager,
                                                      messageManager,
                                                      repositoriesDao);

        replicationsService = new ReplicationsService(storageManager,
                                                      messageManager,
                                                      repositoriesDao,
                                                      replicationsDao,
                                                      repositoriesService);

        nodesService = new NodesService(config,
                                        taskManager,
                                        storageManager,
                                        attributesDao,
                                        nodesDao,
                                        nodeNameProvider,
                                        publishUrisProvider,
                                        nodePingHandler);
    }

    /**
     * Starts the module.
     */
    public void start() {
        repositoriesService.start();
        replicationsService.start();
        nodesService.start();
    }

    /**
     * Properly stops all services.
     */
    public void stop() {
        nodesService.stop();
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
     * @return The nodes service.
     */
    public NodesService getNodesService() {
        return nodesService;
    }
}
