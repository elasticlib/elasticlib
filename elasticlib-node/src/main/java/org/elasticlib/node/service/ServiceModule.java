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
import org.elasticlib.node.components.ComponentsModule;
import org.elasticlib.node.dao.DaoModule;
import org.elasticlib.node.manager.ManagerModule;

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
     * @param daoModule DAO module.
     * @param componentsModule Components module.
     */
    public ServiceModule(Config config,
                         ManagerModule managerModule,
                         DaoModule daoModule,
                         ComponentsModule componentsModule) {

        repositoriesService = new RepositoriesService(managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      componentsModule.getLocalRepositoriesPool());

        replicationsService = new ReplicationsService(managerModule.getStorageManager(),
                                                      managerModule.getMessageManager(),
                                                      daoModule.getReplicationsDao(),
                                                      componentsModule.getRepositoriesProvider(),
                                                      componentsModule.getReplicationAgentsPool());

        nodeService = new NodeService(managerModule.getStorageManager(),
                                      componentsModule.getLocalRepositoriesPool(),
                                      componentsModule.getNodeNameProvider(),
                                      componentsModule.getNodeGuidProvider(),
                                      componentsModule.getPublishUrisProvider());

        remotesService = new RemotesService(config,
                                            managerModule.getTaskManager(),
                                            managerModule.getStorageManager(),
                                            daoModule.getRemotesDao(),
                                            componentsModule.getNodeGuidProvider(),
                                            componentsModule.getNodePingHandler());
    }

    /**
     * Starts the module.
     */
    public void start() {
        replicationsService.start();
        remotesService.start();
    }

    /**
     * Properly stops all services.
     */
    public void stop() {
        remotesService.stop();
        replicationsService.stop();
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
