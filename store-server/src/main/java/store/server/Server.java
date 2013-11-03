package store.server;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.net.URI;
import java.nio.file.Path;
import java.util.logging.Logger;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import store.server.exception.StoreRuntimeException;

/**
 * A Standalone HTTP server. Expose a REST resource on a repository.
 */
public class Server {

    private final HttpServer httpServer;

    /**
     * Constructor.
     *
     * @param home Path to repository's home-directory.
     */
    public Server(Path home) {
        ResourceConfig resourceConfig = new ResourceConfig()
                .register(MultiPartFeature.class)
                .register(new RestResource(new Repository(home)))
                .register(new LoggingFilter(Logger.getGlobal(), false));

        httpServer = GrizzlyHttpServerFactory.createHttpServer(localhost(8080), resourceConfig, false);

        getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                httpServer.stop();
            }
        });
    }

    private static URI localhost(int port) {
        return UriBuilder.fromUri("http:/")
                .host("localhost")
                .port(port)
                .build();
    }

    /**
     * Starts the server.
     */
    public void start() {
        try {
            httpServer.start();

        } catch (IOException ex) {
            throw new StoreRuntimeException(ex);
        }
    }
}
