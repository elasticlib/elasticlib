package store.server.discovery;

import store.common.config.Config;
import store.server.manager.ManagerModule;
import store.server.service.ServiceModule;

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