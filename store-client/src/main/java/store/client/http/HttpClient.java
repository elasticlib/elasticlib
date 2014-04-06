package store.client.http;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.size;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfo.ContentInfoBuilder;
import store.common.ContentInfoTree;
import store.common.Event;
import store.common.IndexEntry;
import static store.common.IoUtil.copy;
import static store.common.IoUtil.copyAndDigest;
import store.common.Mappable;
import store.common.Operation;
import static store.common.SinkOutputStream.sink;
import store.common.hash.Digest;
import store.common.hash.Hash;
import store.common.json.JsonReading;
import static store.common.json.JsonWriting.write;
import static store.common.metadata.MetadataUtil.metadata;

/**
 * HTTP Client.
 */
public class HttpClient implements Closeable {

    private final Display display;
    private final Client client;
    private final WebTarget resource;

    /**
     * Constructor.
     *
     * @param url Server url.
     * @param display Display to use.
     */
    public HttpClient(String url, Display display) {
        this.display = display;

        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        ClientConfig clientConfig = new ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024)
                .connectorProvider(new ApacheConnectorProvider())
                .register(MultiPartFeature.class)
                .register(new HeaderRestoringWriterInterceptor())
                .register(new LoggingFilter(logger, true));

        client = ClientBuilder.newClient(clientConfig);
        resource = client.target(url);
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

    private static <T extends Mappable> T read(Response response, Class<T> clazz) {
        JsonObject json = ensureSuccess(response).readEntity(JsonObject.class);
        return JsonReading.read(json, clazz);
    }

    private static <T extends Mappable> List<T> readAll(Response response, Class<T> clazz) {
        JsonArray array = ensureSuccess(response).readEntity(JsonArray.class);
        return JsonReading.readAll(array, clazz);
    }

    private static CommandResult result(Response response) {
        return read(response, CommandResult.class);
    }

    private static List<String> asStringList(JsonArray array) {
        List<String> list = new ArrayList<>();
        for (JsonString value : array.getValuesAs(JsonString.class)) {
            list.add(value.getString());
        }
        return list;
    }

    public void testConnection() {
        ensureSuccess(resource.path("repositories")
                .request()
                .get())
                .close();
    }

    public void testRepository(String name) {
        ensureSuccess(resource.path("repositories/{name}")
                .resolveTemplate("name", name)
                .request()
                .get())
                .close();
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
            List<ContentInfo> head = head(repository, digest.getHash());
            if (!isDeleted(head)) {
                throw new RequestFailedException("This content is already stored");
            }
            ContentInfo info = new ContentInfoBuilder()
                    .withContent(digest.getHash())
                    .withLength(digest.getLength())
                    .withParents(revs(head))
                    .withMetadata(metadata(filepath))
                    .computeRevisionAndBuild();

            CommandResult firstStepResult = result(resource
                    .path("repositories/{name}/info")
                    .resolveTemplate("name", repository)
                    .request()
                    .post(entity(write(info), MediaType.APPLICATION_JSON_TYPE)));

            if (firstStepResult.isNoOp() || firstStepResult.getOperation() != Operation.CREATE) {
                return firstStepResult;
            }
            try (InputStream inputStream = new LoggingInputStream(display,
                                                                  "Uploading content",
                                                                  newInputStream(filepath),
                                                                  info.getLength())) {
                MultiPart multipart = new FormDataMultiPart()
                        .bodyPart(new StreamDataBodyPart("content",
                                                         inputStream,
                                                         filepath.getFileName().toString(),
                                                         MediaType.APPLICATION_OCTET_STREAM_TYPE));

                return result(resource.path("repositories/{name}/content/{hash}")
                        .resolveTemplate("name", repository)
                        .resolveTemplate("hash", info.getContent())
                        .queryParam("txId", firstStepResult.getTransactionId())
                        .request()
                        .put(entity(multipart, addBoundary(multipart.getMediaType()))));
            }
        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private Digest digest(Path filepath) throws IOException {
        try (InputStream inputStream = new LoggingInputStream(display,
                                                              "Computing content digest",
                                                              newInputStream(filepath),
                                                              size(filepath))) {
            return copyAndDigest(inputStream, sink());
        }
    }

    public CommandResult delete(String repository, Hash hash) {
        List<ContentInfo> head = head(repository, hash);
        if (isDeleted(head)) {
            throw new RequestFailedException("This content is already deleted");
        }
        return result(resource
                .path("repositories/{name}/content/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .queryParam("rev", Joiner.on('-').join(revs(head)))
                .request()
                .delete());
    }

    private List<ContentInfo> head(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/info/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .queryParam("rev", "head")
                .request()
                .get();

        return readAll(response, ContentInfo.class);
    }

    private static Set<Hash> revs(List<ContentInfo> head) {
        Set<Hash> revs = new HashSet<>();
        for (ContentInfo info : head) {
            revs.add(info.getRevision());
        }
        return revs;
    }

    private static boolean isDeleted(List<ContentInfo> head) {
        for (ContentInfo info : head) {
            if (!info.isDeleted()) {
                return false;
            }
        }
        return true;
    }

    public ContentInfoTree getInfoTree(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/info/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .request()
                .get();

        return read(response, ContentInfoTree.class);
    }

    public void get(String repository, Hash hash) {
        Response response = ensureSuccess(resource.path("repositories/{name}/content/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .request()
                .get());

        Path path = Paths.get(fileName(response).or(hash.asHexadecimalString()));
        try (InputStream inputStream = response.readEntity(InputStream.class);
                OutputStream outputStream = new DefferedFileOutputStream(path)) {
            copy(inputStream, outputStream);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private static Optional<String> fileName(Response response) {
        String header = response.getHeaders().getFirst("Content-Disposition").toString();
        if (header == null || header.isEmpty()) {
            return Optional.absent();
        }
        for (String param : Splitter.on(';').trimResults().omitEmptyStrings().split(header)) {
            List<String> parts = Splitter.on('=').trimResults().omitEmptyStrings().splitToList(param);
            if (parts.size() == 2 && parts.get(0).equalsIgnoreCase("filename")) {
                return Optional.of(parts.get(1));
            }
        }
        return Optional.absent();
    }

    public List<IndexEntry> find(String repository, String query, int from, int size) {
        Response response = resource.path("repositories/{name}/index")
                .resolveTemplate("name", repository)
                .queryParam("query", query)
                .queryParam("from", from)
                .queryParam("size", size)
                .request()
                .get();

        return readAll(response, IndexEntry.class);
    }

    public List<ContentInfo> findInfo(String repository, String query, int from, int size) {
        Response response = resource.path("repositories/{name}/info")
                .resolveTemplate("name", repository)
                .queryParam("query", query)
                .queryParam("from", from)
                .queryParam("size", size)
                .request()
                .get();

        return readAll(response, ContentInfo.class);
    }

    public List<Event> history(String repository, boolean asc, long from, int size) {
        Response response = resource.path("repositories/{name}/history")
                .resolveTemplate("name", repository)
                .queryParam("sort", asc ? "asc" : "desc")
                .queryParam("from", from)
                .queryParam("size", size)
                .request()
                .get();

        return readAll(response, Event.class);
    }

    @Override
    public void close() {
        client.close();
    }
}
