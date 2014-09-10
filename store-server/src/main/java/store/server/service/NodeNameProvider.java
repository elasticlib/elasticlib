package store.server.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.server.config.ServerConfig;

/**
 * Provides local node name.
 */
public class NodeNameProvider {

    private static final Logger LOG = LoggerFactory.getLogger(NodeNameProvider.class);
    private final Config config;

    /**
     * Constructor.
     *
     * @param config Config.
     */
    public NodeNameProvider(Config config) {
        this.config = config;
    }

    /**
     * Extract the name of the local node from configuration. If node name is not statically defined in the
     * configuration, try to resolve the machine name.
     *
     * @return The name of the local node.
     */
    public String name() {
        if (config.containsKey(ServerConfig.NODE_NAME)) {
            return config.getString(ServerConfig.NODE_NAME);
        }
        try {
            return InetAddress.getLocalHost().getHostName();

        } catch (UnknownHostException e) {
            LOG.warn("Failed to resolve local host", e);
            return "unknown";
        }
    }
}
