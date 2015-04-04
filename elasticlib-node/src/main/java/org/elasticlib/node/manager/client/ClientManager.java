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

import org.elasticlib.common.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides node HTTP client.
 */
public class ClientManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClientManager.class);

    private final Client client = new Client(new ClientLoggingHandler(LOG));

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
