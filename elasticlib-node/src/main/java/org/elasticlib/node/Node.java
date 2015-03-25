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
package org.elasticlib.node;

import java.io.IOException;
import static java.lang.System.lineSeparator;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.elasticlib.common.config.Config;
import org.elasticlib.node.components.ComponentsModule;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.dao.DaoModule;
import org.elasticlib.node.discovery.DiscoveryModule;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.providers.LoggingFilter;
import org.elasticlib.node.service.NodeService;
import org.elasticlib.node.service.RemotesService;
import org.elasticlib.node.service.ReplicationsService;
import org.elasticlib.node.service.RepositoriesService;
import org.elasticlib.node.service.ServiceModule;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides access to a set of repositories with associated services, over a REST API. May also communicates with others
 * nodes using this same API, as a client.
 */
public class Node {

    private static final Logger LOG = LoggerFactory.getLogger(Node.class);
    private final Path home;
    private final Config config;
    private final ManagerModule managerModule;
    private final DaoModule daoModule;
    private final ComponentsModule componentsModule;
    private final ServiceModule serviceModule;
    private final DiscoveryModule discoveryModule;
    private final HttpServer httpServer;
    private boolean stopped;

    /**
     * Constructor.
     *
     * @param home Path to the node home-directory.
     */
    public Node(Path home) {
        this.home = home;
        config = NodeConfig.load(home.resolve("config.yml"));

        managerModule = new ManagerModule(home, config);
        daoModule = new DaoModule(managerModule);
        componentsModule = new ComponentsModule(config, managerModule, daoModule);
        serviceModule = new ServiceModule(config, managerModule, daoModule, componentsModule);
        discoveryModule = new DiscoveryModule(config, managerModule, serviceModule);

        ResourceConfig resourceConfig = new ResourceConfig()
                .packages("org.elasticlib.node.resources",
                          "org.elasticlib.node.providers")
                .register(new LoggingFilter())
                .register(bindings());

        httpServer = GrizzlyHttpServerFactory.createHttpServer(host(config),
                                                               resourceConfig,
                                                               false);
    }

    private AbstractBinder bindings() {
        return new AbstractBinder() {
            @Override
            protected void configure() {
                bind(serviceModule.getRepositoriesService()).to(RepositoriesService.class);
                bind(serviceModule.getReplicationsService()).to(ReplicationsService.class);
                bind(serviceModule.getNodeService()).to(NodeService.class);
                bind(serviceModule.getRemotesService()).to(RemotesService.class);
            }
        };
    }

    private static URI host(Config config) {
        return UriBuilder.fromUri("http:/")
                .host(config.getString(NodeConfig.HTTP_HOST))
                .port(config.getInt(NodeConfig.HTTP_PORT))
                .path(config.getString(NodeConfig.HTTP_CONTEXT))
                .build();
    }

    /**
     * Starts the node.
     */
    public synchronized void start() {
        LOG.info(startingMessage());
        try {
            managerModule.start();
            componentsModule.start();
            serviceModule.start();
            discoveryModule.start();
            httpServer.start();
            LOG.info("Started");

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String startingMessage() {
        return new StringBuilder()
                .append(about()).append(lineSeparator())
                .append("Starting...").append(lineSeparator())
                .append("Using home: ").append(home).append(lineSeparator())
                .append("Using config:").append(lineSeparator())
                .append(config)
                .toString();
    }

    private static String about() {
        Package pkg = Node.class.getPackage();
        String title = pkg.getImplementationTitle() == null ? "" : pkg.getImplementationTitle();
        String version = pkg.getImplementationVersion() == null ? "" : pkg.getImplementationVersion();
        return String.join("", title, " ", version);
    }

    /**
     * Stops the node.
     */
    public synchronized void stop() {
        if (stopped) {
            return;
        }
        stopped = true;

        LOG.info("Stopping...");
        httpServer.shutdown();
        discoveryModule.stop();
        serviceModule.stop();
        componentsModule.stop();
        managerModule.stop();
        LOG.info("Stopped");
    }
}
