package store.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import static java.nio.file.Files.newInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import static javax.ws.rs.client.Entity.entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
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
import static store.client.DigestUtil.digest;
import store.common.Config;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Digest;
import store.common.Event;
import static store.common.IoUtil.copy;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.readContentInfos;
import static store.common.JsonUtil.readEvents;
import static store.common.JsonUtil.writeConfig;
import static store.common.JsonUtil.writeContentInfo;

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

    private static void ensureOk(Response response) {
        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw new RequestFailedException(response.getStatusInfo().getReasonPhrase());
        }
    }

    public void create(Config config) {
        ensureOk(target.path("create")
                .request()
                .post(entity(writeConfig(config), MediaType.APPLICATION_JSON), Response.class));
    }

    public void drop() {
        ensureOk(target.path("drop")
                .request()
                .method("POST"));
    }

    public void put(Path filepath) {
        Digest digest = digest(filepath);
        ContentInfo info = contentInfo()
                .withHash(digest.getHash())
                .withLength(digest.getLength())
                .build();

        Response response = target.path("contains/{hash}")
                .resolveTemplate("hash", digest.getHash())
                .request()
                .get();

        ensureOk(response);
        if (response.readEntity(Boolean.class)) {
            throw new RequestFailedException("This content is already stored");
        }

        try (InputStream inputStream = new LoggingInputStream("Uploading content",
                                                              newInputStream(filepath),
                                                              digest.getLength())) {

            MultiPart multipart = new FormDataMultiPart()
                    .field("info", writeContentInfo(info), MediaType.APPLICATION_JSON_TYPE)
                    .bodyPart(new StreamDataBodyPart("source",
                                                     inputStream,
                                                     filepath.getFileName().toString(),
                                                     MediaType.APPLICATION_OCTET_STREAM_TYPE));
            ensureOk(target.path("put")
                    .request()
                    .post(entity(multipart, addBoundary(multipart.getMediaType()))));

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    public void delete(String encodedHash) {
        ensureOk(target.path("delete/{hash}")
                .resolveTemplate("hash", encodedHash)
                .request()
                .method("POST"));
    }

    public ContentInfo info(String encodedHash) {
        Response response = target.path("info/{hash}")
                .resolveTemplate("hash", encodedHash)
                .request()
                .get();

        ensureOk(response);
        return readContentInfo(response.readEntity(JsonObject.class));
    }

    public void get(String encodedHash) {
        Response response = target.path("get/{hash}")
                .resolveTemplate("hash", encodedHash)
                .request()
                .get();

        ensureOk(response);

        try (InputStream inputStream = response.readEntity(InputStream.class);
                OutputStream outputStream = new DefferedFileOutputStream(Paths.get(encodedHash))) {
            copy(inputStream, outputStream);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    public List<ContentInfo> find(String query) {
        Response response = target.path("find/{query}")
                .resolveTemplate("query", query)
                .request()
                .get();

        ensureOk(response);
        return readContentInfos(response.readEntity(JsonArray.class));
    }

    public List<Event> history() {
        Response response = target.path("history")
                .request()
                .get();

        ensureOk(response);
        return readEvents(response.readEntity(JsonArray.class));
    }

    @Override
    public void close() {
        client.close();
    }
}
