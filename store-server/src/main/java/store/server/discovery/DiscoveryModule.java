package store.server.discovery;

import store.common.config.Config;
import store.server.ServicesContainer;

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
     * @param servicesContainer Services container.
     */
    public DiscoveryModule(Config config, ServicesContainer servicesContainer) {
        multicastDiscoveryListener = new MulticastDiscoveryListener(config,
                                                                    servicesContainer.getNodesService());

        multicastDiscoveryClient = new MulticastDiscoveryClient(config,
                                                                servicesContainer.getAsyncManager(),
                                                                servicesContainer.getNodesService());

        exchangeDiscoveryClient = new ExchangeDiscoveryClient(config,
                                                              servicesContainer.getAsyncManager(),
                                                              servicesContainer.getNodesService());
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
