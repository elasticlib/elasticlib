package store.client;

import java.io.Closeable;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import static javax.ws.rs.client.Entity.entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import store.common.Config;
import static store.common.json.JsonCodec.encode;

public class StoreClient implements Closeable {

    private Client client = ClientBuilder.newClient();
    private WebTarget target = client.target(localhost(8080));

    private static URI localhost(int port) {
        return UriBuilder.fromUri("http:/")
                .host("localhost")
                .port(port)
                .build();
    }

    public String create(Config config) {
        return target.path("create")
                .request()
                .post(entity(encode(config), MediaType.APPLICATION_JSON), Response.class)
                .getStatusInfo()
                .getReasonPhrase();
    }

    public String put(Path filepath) {
        return ""; // TODO this is a stub
    }

    public String delete(String encodedHash) {
        return ""; // TODO this is a stub
    }

    public String get(String encodedHash) {
        return ""; // TODO this is a stub
    }

    @Override
    public void close() {
        client.close();
    }
}
