package store.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.size;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import static javax.ws.rs.client.Entity.entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.jersey.apache.connector.ApacheConnector;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import static store.client.SinkOutputStream.sink;
import store.common.Config;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Digest;
import static store.common.IoUtil.copy;
import static store.common.IoUtil.copyAndDigest;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.write;

public class StoreClient implements Closeable {

    private final Client client;
    private final WebTarget target;

    public StoreClient() {
        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        ClientConfig clientConfig = new ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024);

        clientConfig.connector(new ApacheConnector(clientConfig))
                .register(MultiPartFeature.class)
                .register(new LoggingFilter(logger, true));

        client = ClientBuilder.newClient(clientConfig);
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
                .post(entity(write(config), MediaType.APPLICATION_JSON), Response.class)
                .getStatusInfo()
                .getReasonPhrase();
    }

    public String drop() {
        return target.path("drop")
                .request()
                .method("POST")
                .getStatusInfo()
                .getReasonPhrase();
    }

    public String put(Path filepath) {
        Digest digest = digest(filepath);
        ContentInfo info = contentInfo()
                .withHash(digest.getHash())
                .withLength(digest.getLength())
                .build();

        try (InputStream inputStream = new LoggingInputStream("Uploading content",
                                                              newInputStream(filepath),
                                                              digest.getLength())) {

            MultiPart multipart = new FormDataMultiPart()
                    .field("info", write(info), MediaType.APPLICATION_JSON_TYPE)
                    .bodyPart(new StreamDataBodyPart("source",
                                                     inputStream,
                                                     filepath.getFileName().toString(),
                                                     MediaType.APPLICATION_OCTET_STREAM_TYPE));
            return target.path("put")
                    .request()
                    .post(entity(multipart, addBoundary(multipart.getMediaType())))
                    .getStatusInfo()
                    .getReasonPhrase();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Digest digest(Path filepath) {
        try (InputStream inputStream = new LoggingInputStream("Computing content digest",
                                                              newInputStream(filepath),
                                                              size(filepath))) {
            return copyAndDigest(inputStream, sink());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String delete(String encodedHash) {
        return target.path("delete/{hash}")
                .resolveTemplate("hash", encodedHash)
                .request()
                .method("POST")
                .getStatusInfo()
                .getReasonPhrase();
    }

    public ContentInfo info(String encodedHash) {
        JsonObject json = target.path("info/{hash}")
                .resolveTemplate("hash", encodedHash)
                .request()
                .get(JsonObject.class);

        return readContentInfo(json);
    }

    public String get(String encodedHash) {
        Response response = target.path("get/{hash}")
                .resolveTemplate("hash", encodedHash)
                .request()
                .get();

        try (InputStream inputStream = response.readEntity(InputStream.class);
                OutputStream outputStream = new DefferedFileOutputStream(Paths.get(encodedHash))) {
            copy(inputStream, outputStream);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return response.getStatusInfo()
                .getReasonPhrase();
    }

    @Override
    public void close() {
        client.close();
    }
}
