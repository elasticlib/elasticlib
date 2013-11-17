package store.server;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import store.server.exception.StoreRuntimeException;
import store.server.resources.IndexesResource;
import store.server.resources.ReplicationsResource;
import store.server.resources.VolumesResource;

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
        Repository repository = new Repository(home);
        ResourceConfig resourceConfig = new ResourceConfig()
                .register(MultiPartFeature.class)
                .register(new IndexesResource(repository))
                .register(new ReplicationsResource(repository))
                .register(new VolumesResource(repository))
                .register(new HttpExceptionMapper())
                .register(new LoggingFilter());

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
