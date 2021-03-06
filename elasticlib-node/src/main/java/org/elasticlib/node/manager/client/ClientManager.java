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
package org.elasticlib.node.manager.client;

import com.google.common.primitives.Ints;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.client.ClientBuilder;
import org.elasticlib.common.config.Config;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import static org.elasticlib.node.config.NodeConfig.CLIENT_CONNECT_TIMEOUT;
import static org.elasticlib.node.config.NodeConfig.CLIENT_MAX_CONNECTIONS;
import static org.elasticlib.node.config.NodeConfig.CLIENT_MAX_CONNECTIONS_PER_ROUTE;
import static org.elasticlib.node.config.NodeConfig.CLIENT_READ_TIMEOUT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides node HTTP client.
 */
public class ClientManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClientManager.class);

    private final Client client;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     */
    public ClientManager(Config config) {
        client = new ClientBuilder()
                .withLoggingHandler(new ClientLoggingHandler(LOG))
                .withConnectTimeout(millis(config, CLIENT_CONNECT_TIMEOUT))
                .withReadTimeout(millis(config, CLIENT_READ_TIMEOUT))
                .withMaxConnections(config.getInt(CLIENT_MAX_CONNECTIONS))
                .withMaxConnectionsPerRoute(config.getInt(CLIENT_MAX_CONNECTIONS_PER_ROUTE))
                .build();
    }

    private static int millis(Config config, String key) {
        if (config.getString(key).isEmpty()) {
            return 0;
        }
        return Ints.checkedCast(MILLISECONDS.convert(duration(config, key), unit(config, key)));
    }

    /**
     * Provides node HTTP client.
     *
     * @return A client.
     */
    public Client getClient() {
        return client;
    }

    /**
     * Stops this manager.
     */
    public void stop() {
        client.close();
    }
}
