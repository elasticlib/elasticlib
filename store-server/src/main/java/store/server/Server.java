package store.server;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.lineSeparator;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.server.config.ServerConfig;
import store.server.discovery.DiscoveryModule;
import store.server.providers.LoggingFilter;
import store.server.service.NodesService;
import store.server.service.RepositoriesService;
import store.server.service.ServiceModule;

/**
 * A Standalone HTTP server.
 */
public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private final ServiceModule serviceModule;
    private final DiscoveryModule discoveryModule;
    private final HttpServer httpServer;

    /**
     * Constructor.
     *
     * @param home Path to server home-directory.
     */
    public Server(Path home) {
        Config config = ServerConfig.load(home.resolve("config.yml"));
        LOG.info(startingMessage(home, config));

        serviceModule = new ServiceModule(home, config);
        discoveryModule = new DiscoveryModule(config, serviceModule);

        ResourceConfig resourceConfig = new ResourceConfig()
                .packages("store.server.resources",
                          "store.server.providers")
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
                discoveryModule.shutdown();
                serviceModule.shutdown();
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
                bind(serviceModule.getNodesService()).to(NodesService.class);
            }
        };
    }

    private static URI host(Config config) {
        return UriBuilder.fromUri("http:/")
                .host(config.getString(ServerConfig.HTTP_HOST))
                .port(config.getInt(ServerConfig.HTTP_PORT))
                .path(config.getString(ServerConfig.HTTP_CONTEXT))
                .build();
    }

    /**
     * Starts the server.
     */
    public void start() {
        try {
            serviceModule.start();
            discoveryModule.start();
            httpServer.start();
            LOG.info("Started");

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
