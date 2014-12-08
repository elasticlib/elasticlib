package org.elasticlib.node.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.elasticlib.common.config.Config;
import org.elasticlib.node.config.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (config.containsKey(NodeConfig.NODE_NAME)) {
            return config.getString(NodeConfig.NODE_NAME);
        }
        try {
            return InetAddress.getLocalHost().getHostName();

        } catch (UnknownHostException e) {
            LOG.warn("Failed to resolve local host", e);
            return "unknown";
        }
    }
}
