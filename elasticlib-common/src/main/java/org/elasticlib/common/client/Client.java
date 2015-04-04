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
package org.elasticlib.common.client;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.ClientBuilder;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * A node HTTP client.
 */
public class Client implements AutoCloseable {

    private final javax.ws.rs.client.Client client;

    /**
     * Constructor.
     *
     * @param loggingHandler Logging handler.
     */
    public Client(LoggingHandler loggingHandler) {
        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        ClientConfig clientConfig = new ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024)
                .connectorProvider(new ApacheConnectorProvider())
                .register(MultiPartFeature.class)
                .register(new HeaderRestoringWriterInterceptor())
                .register(new LoggingFilter(logger, true))
                .register(new ClientLoggingFilter(loggingHandler));

        client = ClientBuilder.newClient(clientConfig);
    }

    /**
     * Provides a base API on a given node.
     *
     * @param uri Node base URI.
     * @return a new base API instance.
     */
    public ClientTarget target(URI uri) {
        return new ClientTarget(client.target(uri));
    }

    @Override
    public void close() {
        client.close();
    }
}
