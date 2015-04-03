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

import java.net.URI;
import org.elasticlib.common.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides node API clients.
 */
public class ClientsManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClientsManager.class);
    private static final ClientLoggingHandler LOGGING_HANDLER = new ClientLoggingHandler(LOG);

    /**
     * Provides a client targeting node at supplied URI.
     *
     * @param uri A node URI.
     * @return A client on this node.
     */
    public Client getClient(URI uri) {
        return new Client(uri, LOGGING_HANDLER);
    }
}
