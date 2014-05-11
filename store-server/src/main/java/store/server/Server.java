package store.server;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.net.URI;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import store.common.config.Config;
import store.server.service.RepositoriesService;

/**
 * A Standalone HTTP server.
 */
public class Server {

    private final Config config;
    private final HttpServer httpServer;
    private final RepositoriesService repositoriesService;

    /**
     * Constructor.
     *
     * @param home Path to repository's home-directory.
     */
    public Server(Path home) {
        config = ServerConfig.load(home.resolve("config.yml"));
        repositoriesService = new RepositoriesService(home);

        ResourceConfig resourceConfig = new ResourceConfig()
                .packages("store.server.resources",
                          "store.server.providers")
                .register(loggingFilter())
                .register(bindings());

        httpServer = GrizzlyHttpServerFactory.createHttpServer(host(), resourceConfig, false);

        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                httpServer.shutdown();
                repositoriesService.close();
            }
        });
    }

    private LoggingFilter loggingFilter() {
        Logger logger = Logger.getLogger(LoggingFilter.class.getName());
        if (!config.getBoolean(ServerConfig.LOG_LOGGING_FILTER)) {
            logger.setLevel(Level.OFF);
            return new LoggingFilter(logger, false);
        }
        if (config.getBoolean(ServerConfig.LOG_PRINT_ENTITY)) {
            return new LoggingFilter(logger, config.getInt(ServerConfig.LOG_MAX_ENTITY_SIZE));
        }
        return new LoggingFilter(logger, false);
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
