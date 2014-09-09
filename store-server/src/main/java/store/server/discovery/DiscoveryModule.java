package store.server.discovery;

import store.common.config.Config;
import store.server.service.ServiceModule;

/**
 * Regroups discovery components.
 */
public class DiscoveryModule {

    private final MulticastDiscoveryListener multicastDiscoveryListener;
    private final MulticastDiscoveryClient multicastDiscoveryClient;
    private final ExchangeDiscoveryClient exchangeDiscoveryClient;

    /**
     * Constructor.
     *
     * @param config Server config.
     * @param serviceModule Service module.
     */
    public DiscoveryModule(Config config, ServiceModule serviceModule) {
        multicastDiscoveryListener = new MulticastDiscoveryListener(config,
                                                                    serviceModule.getNodesService());

        multicastDiscoveryClient = new MulticastDiscoveryClient(config,
                                                                serviceModule.getAsyncManager(),
                                                                serviceModule.getNodesService());

        exchangeDiscoveryClient = new ExchangeDiscoveryClient(config,
                                                              serviceModule.getAsyncManager(),
                                                              serviceModule.getNodesService());
    }

    /**
     * Start the module.
     */
    public void start() {
        multicastDiscoveryListener.start();
        multicastDiscoveryClient.start();
        exchangeDiscoveryClient.start();
    }

    /**
     * Properly shutdown the module and release underlying ressources.
     */
    public void shutdown() {
        exchangeDiscoveryClient.shutdown();
        multicastDiscoveryClient.shutdown();
        multicastDiscoveryListener.shutdown();
    }
}
