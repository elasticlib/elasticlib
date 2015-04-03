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
package org.elasticlib.node.discovery;

import org.elasticlib.common.config.Config;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.service.ServiceModule;

/**
 * Regroups discovery components.
 */
public class DiscoveryModule {

    private final MulticastDiscoveryListener multicastDiscoveryListener;
    private final MulticastDiscoveryClient multicastDiscoveryClient;
    private final UnicastDiscoveryClient unicastDiscoveryClient;

    /**
     * Constructor.
     *
     * @param config Server config.
     * @param managerModule Manager module.
     * @param serviceModule Service module.
     */
    public DiscoveryModule(Config config, ManagerModule managerModule, ServiceModule serviceModule) {
        multicastDiscoveryListener = new MulticastDiscoveryListener(config,
                                                                    serviceModule.getNodeService());

        multicastDiscoveryClient = new MulticastDiscoveryClient(config,
                                                                managerModule.getTaskManager(),
                                                                serviceModule.getRemotesService());

        unicastDiscoveryClient = new UnicastDiscoveryClient(config,
                                                            managerModule.getClientsManager(),
                                                            managerModule.getTaskManager(),
                                                            serviceModule.getNodeService(),
                                                            serviceModule.getRemotesService());
    }

    /**
     * Starts the module.
     */
    public void start() {
        multicastDiscoveryListener.start();
        multicastDiscoveryClient.start();
        unicastDiscoveryClient.start();
    }

    /**
     * Properly stops the module and release underlying ressources.
     */
    public void stop() {
        unicastDiscoveryClient.stop();
        multicastDiscoveryClient.stop();
        multicastDiscoveryListener.stop();
    }
}
