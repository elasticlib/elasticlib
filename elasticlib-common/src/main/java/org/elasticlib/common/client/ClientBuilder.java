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

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheClientProperties;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

/**
 * Node HTTP client builder.
 */
public class ClientBuilder {

    private final PoolingHttpClientConnectionManager connectionManager;
    private final ClientConfig config;

    /**
     * Constructor
     */
    public ClientBuilder() {
        connectionManager = new PoolingHttpClientConnectionManager();
        config = new ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 8192)
                .property(ApacheClientProperties.CONNECTION_MANAGER, connectionManager)
                .connectorProvider(new ApacheConnectorProvider())
                .register(MultiPartFeature.class)
                .register(new HeaderRestoringWriterInterceptor());
    }

    /**
     * Set logging handler.
     *
     * @param loggingHandler Logging handler.
     * @return This builder instance.
     */
    public ClientBuilder withLoggingHandler(LoggingHandler loggingHandler) {
        config.register(new ClientLoggingFilter(loggingHandler));
        return this;
    }

    /**
     * Set connect timeout.
     *
     * @param timeout Timeout in milliseconds
     * @return This builder instance.
     */
    public ClientBuilder withConnectTimeout(int timeout) {
        config.property(ClientProperties.CONNECT_TIMEOUT, timeout);
        return this;
    }

    /**
     * Set read timeout.
     *
     * @param timeout Timeout in milliseconds
     * @return This builder instance.
     */
    public ClientBuilder withReadTimeout(int timeout) {
        config.property(ClientProperties.READ_TIMEOUT, timeout);
        return this;
    }

    /**
     * Set the total maximum number of connections.
     *
     * @param max Maximum number of connections.
     * @return This builder instance.
     */
    public ClientBuilder withMaxConnections(int max) {
        connectionManager.setMaxTotal(max);
        return this;
    }

    /**
     * Set the maximum number of connections per host.
     *
     * @param max Maximum number of connections.
     * @return This builder instance.
     */
    public ClientBuilder withMaxConnectionsPerRoute(int max) {
        connectionManager.setDefaultMaxPerRoute(max);
        return this;
    }

    /**
     * Build a new client.
     *
     * @return A new client instance.
     */
    public Client build() {
        return new Client(config);
    }
}
