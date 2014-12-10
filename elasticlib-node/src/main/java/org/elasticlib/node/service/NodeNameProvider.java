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
