package store.server;

import java.io.IOException;
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

public class StoreServer {

    private final HttpServer httpServer;

    public StoreServer(Path home) {
        ResourceConfig resourceConfig = new ResourceConfig()
                .register(MultiPartFeature.class)
                .register(new StoreResource(new StoreManager(home)))
                .register(new LoggingFilter(Logger.getGlobal(), false));

        httpServer = GrizzlyHttpServerFactory.createHttpServer(localhost(8080), resourceConfig, false);

        Runtime.getRuntime()
                .addShutdownHook(new Thread() {
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

    public void start() {
        try {
            httpServer.start();

        } catch (IOException ex) {
            throw new StoreRuntimeException(ex);
        }
    }
}
