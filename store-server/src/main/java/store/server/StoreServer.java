package store.server;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import store.server.exception.StoreRuntimeException;

public class StoreServer {

    private final HttpServer httpServer;

    public StoreServer(Path home) {
        StoreManager.init(home);
        ResourceConfig resourceConfig = new ResourceConfig(StoreResource.class);
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