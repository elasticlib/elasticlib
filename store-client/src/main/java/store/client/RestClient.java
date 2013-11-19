package store.client;

import com.google.common.base.Joiner;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import static java.nio.file.Files.newInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
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
import store.common.ContentInfo;
import static store.common.ContentInfo.contentInfo;
import store.common.Digest;
import store.common.Event;
import store.common.Hash;
import static store.common.IoUtil.copy;
import static store.common.JsonUtil.readContentInfo;
import static store.common.JsonUtil.readEvents;
import static store.common.JsonUtil.writeContentInfo;
import static store.common.MetadataUtil.metadata;
import static store.common.Properties.Common.CAPTURE_DATE;

/**
 * REST Client.
 */
public class RestClient implements Closeable {

    private final Client client;
    private final WebTarget target;
    private String volume;

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

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    private Response ensureSuccess(Response response) {
        if (response.getStatus() >= 300) {
            if (response.hasEntity() && response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
                String reason = response.getStatusInfo().getReasonPhrase();
                String message = response.readEntity(JsonObject.class).getString("error");
                throw new RequestFailedException(Joiner.on(" - ").join(reason, message));
            }
            throw new RequestFailedException(response.getStatusInfo().getReasonPhrase());
        }
        return response;
    }

    public void createVolume(Path path) {
        JsonObject json = createObjectBuilder()
                .add("path", path.toAbsolutePath().toString())
                .build();

        ensureSuccess(target.path("volumes")
                .request()
                .post(Entity.json(json)));
    }

    public void dropVolume(String name) {
        ensureSuccess(target.path("volumes/{name}")
                .resolveTemplate("name", name)
                .request()
                .delete());
    }

    public void createIndex(Path path, String volumeName) {
        JsonObject json = createObjectBuilder()
                .add("path", path.toAbsolutePath().toString())
                .add("volume", volumeName)
                .build();

        ensureSuccess(target.path("indexes")
                .request()
                .post(Entity.json(json)));
    }

    public void dropIndex(String name) {
        ensureSuccess(target.path("indexes/{name}")
                .resolveTemplate("name", name)
                .request()
                .delete());
    }

    public void sync(String source, String destination) {
        JsonObject json = createObjectBuilder()
                .add("source", source)
                .add("destination", destination)
                .build();

        ensureSuccess(target.path("replications")
                .request()
                .post(Entity.json(json)));
    }

    public void unsync(String source, String destination) {
        ensureSuccess(target.path("replications")
                .queryParam("source", source)
                .queryParam("destination", destination)
                .request()
                .delete());
    }

    public void start(String name) {
        setStarted(name, true);
    }

    public void stop(String name) {
        setStarted(name, false);
    }

    public void setStarted(String name, boolean value) {
        JsonObject json = createObjectBuilder()
                .add("started", value)
                .build();

        ensureSuccess(target.path("volumes/{name}")
                .resolveTemplate("name", name)
                .request()
                .post(Entity.json(json)));
    }

    public void put(Path filepath) {
        Digest digest = digest(filepath);
        ContentInfo info = contentInfo()
                .withHash(digest.getHash())
                .withLength(digest.getLength())
                .withMetadata(metadata(filepath))
                .with(CAPTURE_DATE.key(), new Date())
                .build();

        Response response = target.path("volumes/{name}/info/{hash}")
                .resolveTemplate("name", volume)
                .resolveTemplate("hash", digest.getHash())
                .request()
                .head();

        if (response.getStatus() == Status.OK.getStatusCode()) {
            throw new RequestFailedException("This content is already stored");
        }

        try (InputStream inputStream = new LoggingInputStream("Uploading content",
                                                              newInputStream(filepath),
                                                              digest.getLength())) {

            MultiPart multipart = new FormDataMultiPart()
                    .field("info", writeContentInfo(info), MediaType.APPLICATION_JSON_TYPE)
                    .bodyPart(new StreamDataBodyPart("content",
                                                     inputStream,
                                                     filepath.getFileName().toString(),
                                                     MediaType.APPLICATION_OCTET_STREAM_TYPE));
            ensureSuccess(target.path("volumes/{name}/content")
                    .resolveTemplate("name", volume)
                    .request()
                    .post(entity(multipart, addBoundary(multipart.getMediaType()))));

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    public void delete(Hash hash) {
        ensureSuccess(target.path("volumes/{name}/content/{hash}")
                .resolveTemplate("name", volume)
                .resolveTemplate("hash", hash)
                .request()
                .delete());
    }

    public ContentInfo info(Hash hash) {
        Response response = target.path("volumes/{name}/info/{hash}")
                .resolveTemplate("name", volume)
                .resolveTemplate("hash", hash)
                .request()
                .get();

        return readContentInfo(ensureSuccess(response).readEntity(JsonObject.class));
    }

    public void get(Hash hash) {
        Response response = target.path("volumes/{name}/content/{hash}")
                .resolveTemplate("name", volume)
                .resolveTemplate("hash", hash)
                .request()
                .get();

        try (InputStream inputStream = ensureSuccess(response).readEntity(InputStream.class);
                OutputStream outputStream = new DefferedFileOutputStream(Paths.get(hash.encode()))) {
            copy(inputStream, outputStream);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    public List<ContentInfo> find(String query) {
        return Collections.emptyList(); // TODO this is a stub !
    }

    public List<Event> history(long from, int size) {
        // TODO add ASC / DESC support
        Response response = target.path("volumes/{name}/history")
                .resolveTemplate("name", volume)
                .queryParam("from", from)
                .queryParam("size", size)
                .request()
                .get();

        return readEvents(ensureSuccess(response).readEntity(JsonArray.class));
    }

    @Override
    public void close() {
        client.close();
    }
}
