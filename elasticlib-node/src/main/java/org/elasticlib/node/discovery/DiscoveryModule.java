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
                                                                    serviceModule.getNodesService());

        multicastDiscoveryClient = new MulticastDiscoveryClient(config,
                                                                managerModule.getTaskManager(),
                                                                serviceModule.getNodesService());

        unicastDiscoveryClient = new UnicastDiscoveryClient(config,
                                                            managerModule.getTaskManager(),
                                                            serviceModule.getNodesService());
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
