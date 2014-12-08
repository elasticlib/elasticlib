package org.elasticlib.node;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.lineSeparator;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.elasticlib.common.config.Config;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.discovery.DiscoveryModule;
import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.providers.LoggingFilter;
import org.elasticlib.node.service.NodesService;
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
    private final ManagerModule managerModule;
    private final ServiceModule serviceModule;
    private final DiscoveryModule discoveryModule;
    private final HttpServer httpServer;

    /**
     * Constructor.
     *
     * @param home Path to the node home-directory.
     */
    public Node(Path home) {
        Config config = NodeConfig.load(home.resolve("config.yml"));
        LOG.info(startingMessage(home, config));

        managerModule = new ManagerModule(home, config);
        serviceModule = new ServiceModule(config, managerModule);
        discoveryModule = new DiscoveryModule(config, managerModule, serviceModule);

        ResourceConfig resourceConfig = new ResourceConfig()
                .packages("org.elasticlib.node.resources",
                          "org.elasticlib.node.providers")
                .register(new LoggingFilter())
                .register(bindings());

        httpServer = GrizzlyHttpServerFactory.createHttpServer(host(config),
                                                               resourceConfig,
                                                               false);

        getRuntime().addShutdownHook(new Thread("shutdown") {
            @Override
            public void run() {
                LOG.info("Stopping...");
                httpServer.shutdown();
                discoveryModule.stop();
                serviceModule.stop();
                managerModule.stop();
                LOG.info("Stopped");
            }
        });
    }

    private static String startingMessage(Path home, Config config) {
        return new StringBuilder()
                .append("Starting...").append(lineSeparator())
                .append("Using home: ").append(home).append(lineSeparator())
                .append("Using config:").append(lineSeparator())
                .append(config)
                .toString();
    }

    private AbstractBinder bindings() {
        return new AbstractBinder() {
            @Override
            protected void configure() {
                bind(serviceModule.getRepositoriesService()).to(RepositoriesService.class);
                bind(serviceModule.getReplicationsService()).to(ReplicationsService.class);
                bind(serviceModule.getNodesService()).to(NodesService.class);
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
    public void start() {
        try {
            managerModule.start();
            serviceModule.start();
            discoveryModule.start();
            httpServer.start();
            LOG.info("Started");

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}