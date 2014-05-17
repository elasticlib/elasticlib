package store.server;

import store.server.config.ServerConfig;
import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.server.providers.LoggingFilter;
import store.server.service.RepositoriesService;

/**
 * A Standalone HTTP server.
 */
public class Server {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Server.class);
    private final Config config;
    private final HttpServer httpServer;
    private final RepositoriesService repositoriesService;

    /**
     * Constructor.
     *
     * @param home Path to repository's home-directory.
     */
    public Server(Path home) {
        LOG.info("Starting...");
        config = ServerConfig.load(home.resolve("config.yml"));
        repositoriesService = new RepositoriesService(home, config);

        ResourceConfig resourceConfig = new ResourceConfig()
                .packages("store.server.resources",
                          "store.server.providers")
                .register(new LoggingFilter())
                .register(bindings());

        httpServer = GrizzlyHttpServerFactory.createHttpServer(host(), resourceConfig, false);

        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("Stopping...");
                httpServer.shutdown();
                repositoriesService.close();
            }
        });
    }

    private URI host() {
        return UriBuilder.fromUri("http:/")
                .host(config.getString(ServerConfig.WEB_HOST))
                .port(config.getInt(ServerConfig.WEB_PORT))
                .path(config.getString(ServerConfig.WEB_CONTEXT))
                .build();
    }

    private AbstractBinder bindings() {
        return new AbstractBinder() {
            @Override
            protected void configure() {
                bind(repositoriesService).to(RepositoriesService.class);
            }
        };
    }

    /**
     * Starts the server.
     */
    public void start() {
        try {
            httpServer.start();

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
