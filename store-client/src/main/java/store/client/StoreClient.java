package store.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import static javax.ws.rs.client.Entity.entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import store.common.Config;
import store.common.hash.Digest;
import store.common.info.ContentInfo;
import static store.common.info.ContentInfo.contentInfo;
import static store.common.json.JsonCodec.encode;

public class StoreClient implements Closeable {

    private final Client client;
    private final WebTarget target;

    public StoreClient() {
        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        client = ClientBuilder.newBuilder()
                .register(MultiPartFeature.class)
                .register(new LoggingFilter(logger, true))
                .build();

        target = client.target(localhost(8080));
    }

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
        Digest digest = digest(filepath);
        ContentInfo info = contentInfo()
                .withHash(digest.getHash())
                .withLength(digest.getLength())
                .build();

        MultiPart multipart = new FormDataMultiPart()
                .field("info", encode(info), MediaType.APPLICATION_JSON_TYPE)
                .bodyPart(new FileDataBodyPart("source", filepath.toFile(), MediaType.APPLICATION_OCTET_STREAM_TYPE));

        return target.path("put")
                .request()
                .post(entity(multipart, multipart.getMediaType()))
                .getStatusInfo()
                .getReasonPhrase();
    }

    private static Digest digest(Path filepath) {
        try (InputStream inputStream = Files.newInputStream(filepath)) {
            return Digest.of(inputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
