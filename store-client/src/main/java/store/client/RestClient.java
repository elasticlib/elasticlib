package store.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import static java.nio.file.Files.newInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
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
import static store.client.MetadataUtil.metadata;
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Digest;
import store.common.Event;
import store.common.Hash;
import static store.common.IoUtil.copy;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.readContentInfos;
import static store.common.JsonUtil.readEvents;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.Properties.Common.CAPTURE_DATE;

/**
 * REST Client.
 */
public class RestClient implements Closeable {

    private static final String POST = "POST";
    private final Client client;
    private final WebTarget target;

    /**
     * Constructor.
     */
    public RestClient() {
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

    private Response ensureOk(Response response) {
        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw new RequestFailedException(response.getStatusInfo().getReasonPhrase());
        }
        return response;
    }

    public void createVolume(Path path) {
        ensureOk(target.path("createVolume/{path}")
                .resolveTemplate("path", path.toAbsolutePath())
                .request()
                .method(POST));
    }

    public void dropVolume(String name) {
        ensureOk(target.path("dropVolume/{name}")
                .resolveTemplate("name", name)
                .request()
                .method(POST));
    }

    public void createIndex(Path path, String volumeName) {
        ensureOk(target.path("createIndex/{path}/{name}")
                .resolveTemplate("path", path.toAbsolutePath())
                .resolveTemplate("name", volumeName)
                .request()
                .method(POST));
    }

    public void dropIndex(String name) {
        ensureOk(target.path("dropIndex/{name}")
                .resolveTemplate("name", name)
                .request()
                .method(POST));
    }

    public void sync(String source, String destination) {
        ensureOk(target.path("sync/{source}/{destination}")
                .resolveTemplate("source", source)
                .resolveTemplate("destination", destination)
                .request()
                .method(POST));
    }

    public void unsync(String source, String destination) {
        ensureOk(target.path("unsync/{source}/{destination}")
                .resolveTemplate("source", source)
                .resolveTemplate("destination", destination)
                .request()
                .method(POST));
    }

    public void start(String name) {
        ensureOk(target.path("start/{name}")
                .resolveTemplate("name", name)
                .request()
                .method(POST));
    }

    public void stop(String name) {
        ensureOk(target.path("stop/{name}")
                .resolveTemplate("name", name)
                .request()
                .method(POST));
    }

    public void put(Path filepath) {
        Digest digest = digest(filepath);
        ContentInfo info = contentInfo()
                .withHash(digest.getHash())
                .withLength(digest.getLength())
                .withMetadata(metadata(filepath))
                .with(CAPTURE_DATE.key(), new Date())
                .build();

        Response response = target.path("contains/{hash}")
                .resolveTemplate("hash", digest.getHash())
                .request()
                .get();

        if (ensureOk(response).readEntity(Boolean.class)) {
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

    public void delete(Hash hash) {
        ensureOk(target.path("delete/{hash}")
                .resolveTemplate("hash", hash)
                .request()
                .method(POST));
    }

    public ContentInfo info(Hash hash) {
        Response response = target.path("info/{hash}")
                .resolveTemplate("hash", hash)
                .request()
                .get();

        return readContentInfo(ensureOk(response).readEntity(JsonObject.class));
    }

    public void get(Hash hash) {
        Response response = target.path("get/{hash}")
                .resolveTemplate("hash", hash)
                .request()
                .get();

        try (InputStream inputStream = ensureOk(response).readEntity(InputStream.class);
                OutputStream outputStream = new DefferedFileOutputStream(Paths.get(hash.encode()))) {
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

        return readContentInfos(ensureOk(response).readEntity(JsonArray.class));
    }

    public List<Event> history(boolean chronological, long first, int number) {
        Response response = target.path("history/{chronological}/{first}/{number}")
                .resolveTemplate("chronological", chronological)
                .resolveTemplate("first", first)
                .resolveTemplate("number", number)
                .request()
                .get();

        return readEvents(ensureOk(response).readEntity(JsonArray.class));
    }

    @Override
    public void close() {
        client.close();
    }
}
