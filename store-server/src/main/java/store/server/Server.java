package store.server;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.lineSeparator;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import store.server.async.AsyncManager;
import store.server.config.ServerConfig;
import store.server.exception.WriteException;
import store.server.providers.LoggingFilter;
import store.server.service.RepositoriesService;
import store.server.storage.StorageManager;

/**
 * A Standalone HTTP server.
 */
public class Server {

    private static final String STORAGE = "storage";
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Server.class);
    private final Config config;
    private final RepositoriesService repositoriesService;
    private final HttpServer httpServer;

    /**
     * Constructor.
     *
     * @param home Path to repository's home-directory.
     */
    public Server(Path home) {
        config = ServerConfig.load(home.resolve("config.yml"));
        LOG.info(startingMessage(home, config));

        AsyncManager asyncManager = new AsyncManager(config);
        StorageManager storageManager = newStorageManager(home.resolve(STORAGE), config, asyncManager);
        repositoriesService = new RepositoriesService(config, asyncManager, storageManager);

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

    private static String startingMessage(Path home, Config config) {
        return new StringBuilder()
                .append("Starting...").append(lineSeparator())
                .append("Using home: ").append(home).append(lineSeparator())
                .append("Using config:").append(lineSeparator())
                .append(config)
                .toString();
    }

    private static StorageManager newStorageManager(Path path, Config config, AsyncManager asyncManager) {
        try {
            if (!Files.exists(path)) {
                Files.createDirectory(path);
            }
        } catch (IOException e) {
            throw new WriteException(e);
        }
        return new StorageManager(Server.class.getSimpleName(), path, config, asyncManager);
    }

    private AbstractBinder bindings() {
        return new AbstractBinder() {
            @Override
            protected void configure() {
                bind(repositoriesService).to(RepositoriesService.class);
            }
        };
    }

    private URI host() {
        return UriBuilder.fromUri("http:/")
                .host(config.getString(ServerConfig.WEB_HOST))
                .port(config.getInt(ServerConfig.WEB_PORT))
                .path(config.getString(ServerConfig.WEB_CONTEXT))
                .build();
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
