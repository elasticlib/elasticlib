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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import static javax.ws.rs.client.Entity.entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import static store.client.DigestUtil.digest;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.Digest;
import store.common.Event;
import store.common.Hash;
import static store.common.IoUtil.copy;
import store.common.Operation;
import static store.common.json.JsonUtil.readCommandResult;
import static store.common.json.JsonUtil.readContentInfo;
import static store.common.json.JsonUtil.readContentInfos;
import static store.common.json.JsonUtil.readEvents;
import static store.common.json.JsonUtil.readHashes;
import static store.common.json.JsonUtil.writeContentInfo;
import static store.common.metadata.MetadataUtil.metadata;

/**
 * REST Client.
 */
public class RestClient implements Closeable {

    private final Client client;
    private final WebTarget resource;

    /**
     * Constructor.
     */
    public RestClient() {
        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        ClientConfig clientConfig = new ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024)
                .connectorProvider(new ApacheConnectorProvider())
                .register(MultiPartFeature.class)
                .register(new HeaderRestoringWriterInterceptor())
                .register(new LoggingFilter(logger, true));

        client = ClientBuilder.newClient(clientConfig);
        resource = client.target(localhost(8080));
    }

    private static URI localhost(int port) {
        return UriBuilder.fromUri("http:/")
                .host("localhost")
                .port(port)
                .build();
    }

    private static Response ensureSuccess(Response response) {
        if (response.getStatus() >= 400) {
            if (response.hasEntity() && response.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
                String reason = response.getStatusInfo().getReasonPhrase();
                String message = response.readEntity(JsonObject.class).getString("error");
                throw new RequestFailedException(Joiner.on(" - ").join(reason, message));
            }
            throw new RequestFailedException(response.getStatusInfo().getReasonPhrase());
        }
        return response;
    }

    private static List<String> asStringList(JsonArray array) {
        List<String> list = new ArrayList<>();
        for (JsonString value : array.getValuesAs(JsonString.class)) {
            list.add(value.getString());
        }
        return list;
    }

    public void createRepository(Path path) {
        JsonObject json = createObjectBuilder()
                .add("path", path.toAbsolutePath().toString())
                .build();

        ensureSuccess(resource.path("repositories")
                .request()
                .post(Entity.json(json)));
    }

    public void dropRepository(String name) {
        ensureSuccess(resource.path("repositories/{name}")
                .resolveTemplate("name", name)
                .request()
                .delete());
    }

    public List<String> listRepositories() {
        Response response = ensureSuccess(resource.path("repositories")
                .request()
                .get());

        return asStringList(response.readEntity(JsonArray.class));
    }

    public void createReplication(String source, String target) {
        JsonObject json = createObjectBuilder()
                .add("source", source)
                .add("target", target)
                .build();

        ensureSuccess(resource.path("replications")
                .request()
                .post(Entity.json(json)));
    }

    public void dropReplication(String source, String target) {
        ensureSuccess(resource.path("replications")
                .queryParam("source", source)
                .queryParam("target", target)
                .request()
                .delete());
    }

    public CommandResult put(String repository, Path filepath) {
        try {
            Digest digest = digest(filepath);
            ContentInfo info = new ContentInfoBuilder()
                    .withHash(digest.getHash())
                    .withLength(digest.getLength())
                    .withMetadata(metadata(filepath))
                    .computeRevAndBuild();

            if (info.getLength() < 4096) {
                return putInOneStep(repository, filepath, info);
            } else {
                return putInTwoStep(repository, filepath, info);
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private CommandResult putInOneStep(String repository, Path filepath, ContentInfo info) throws IOException {
        try (InputStream inputStream = new LoggingInputStream("Uploading content",
                                                              newInputStream(filepath),
                                                              info.getLength())) {
            MultiPart multipart = new FormDataMultiPart()
                    .field("info", writeContentInfo(info), MediaType.APPLICATION_JSON_TYPE)
                    .bodyPart(new StreamDataBodyPart("content",
                                                     inputStream,
                                                     filepath.getFileName().toString(),
                                                     MediaType.APPLICATION_OCTET_STREAM_TYPE));

            return result(resource.path("repositories/{name}/content")
                    .resolveTemplate("name", repository)
                    .request()
                    .post(entity(multipart, addBoundary(multipart.getMediaType()))));
        }
    }

    private CommandResult putInTwoStep(String repository, Path filepath, ContentInfo info) throws IOException {
        CommandResult firstStepResult = result(resource
                .path("repositories/{name}/info")
                .resolveTemplate("name", repository)
                .request()
                .post(entity(writeContentInfo(info), MediaType.APPLICATION_JSON_TYPE)));

        if (firstStepResult.isNoOp() || firstStepResult.getOperation() != Operation.CREATE) {
            return firstStepResult;
        }
        try (InputStream inputStream = new LoggingInputStream("Uploading content",
                                                              newInputStream(filepath),
                                                              info.getLength())) {
            MultiPart multipart = new FormDataMultiPart()
                    .bodyPart(new StreamDataBodyPart("content",
                                                     inputStream,
                                                     filepath.getFileName().toString(),
                                                     MediaType.APPLICATION_OCTET_STREAM_TYPE));

            return result(resource.path("repositories/{name}/content/{hash}")
                    .resolveTemplate("name", repository)
                    .resolveTemplate("hash", info.getHash())
                    .queryParam("txId", firstStepResult.getTransactionId())
                    .request()
                    .put(entity(multipart, addBoundary(multipart.getMediaType()))));
        }
    }

    public CommandResult delete(String repository, Hash hash) {
        return result(resource.path("repositories/{name}/content/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .request()
                .delete());
    }

    private static CommandResult result(Response response) {
        return readCommandResult(ensureSuccess(response).readEntity(JsonObject.class));
    }

    public ContentInfo info(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/info/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .request()
                .get();

        return readContentInfo(ensureSuccess(response).readEntity(JsonObject.class));
    }

    public void get(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/content/{hash}")
                .resolveTemplate("name", repository)
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

    public List<Hash> find(String repository, String query, int from, int size) {
        Response response = resource.path("repositories/{name}/index")
                .resolveTemplate("name", repository)
                .queryParam("query", query)
                .queryParam("from", from)
                .queryParam("size", size)
                .request()
                .get();

        return readHashes(ensureSuccess(response).readEntity(JsonArray.class));
    }

    public List<ContentInfo> findInfo(String repository, String query, int from, int size) {
        Response response = resource.path("repositories/{name}/info")
                .resolveTemplate("name", repository)
                .queryParam("query", query)
                .queryParam("from", from)
                .queryParam("size", size)
                .request()
                .get();

        return readContentInfos(ensureSuccess(response).readEntity(JsonArray.class));
    }

    public List<Event> history(String repository, boolean asc, long from, int size) {
        Response response = resource.path("repositories/{name}/history")
                .resolveTemplate("name", repository)
                .queryParam("sort", asc ? "asc" : "desc")
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
