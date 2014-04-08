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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import store.common.RepositoryDef;
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

    /**
     * Tests connection to current server. Fails if connection is down.
     */
    void testConnection() {
        ensureSuccess(resource.request().get()).close();
    }

    /**
     * Tests that supplied repository really exists. Fails if it does not.
     *
     * @param name Repository name.
     */
    void testRepository(String name) {
        ensureSuccess(resource.path("repositories/{name}")
                .resolveTemplate("name", name)
                .request()
                .get())
                .close();
    }

    /**
     * Creates a new repository at supplied path.
     *
     * @param path Repository path (from server perspective).
     */
    public void createRepository(Path path) {
        JsonObject json = createObjectBuilder()
                .add("path", path.toAbsolutePath().toString())
                .build();

        ensureSuccess(resource.path("repositories")
                .request()
                .post(Entity.json(json)));
    }

    /**
     * Deletes an existing repository.
     *
     * @param name Repository name.
     */
    public void dropRepository(String name) {
        ensureSuccess(resource.path("repositories/{name}")
                .resolveTemplate("name", name)
                .request()
                .delete());
    }

    /**
     * Lists existing repositories.
     *
     * @return A list of repository definitions.
     */
    public List<RepositoryDef> listRepositoryDefs() {
        Response response = resource.path("repositories")
                .request()
                .get();

        return readAll(response, RepositoryDef.class);
    }

    /**
     * Lists existing replications.
     *
     * @return A list of replication definitions.
     */
    public List<RepositoryDef> listReplicationDefs() {
        Response response = resource.path("replications")
                .request()
                .get();

        return readAll(response, RepositoryDef.class);
    }

    /**
     * Creates a new replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void createReplication(String source, String target) {
        JsonObject json = createObjectBuilder()
                .add("source", source)
                .add("target", target)
                .build();

        ensureSuccess(resource.path("replications")
                .request()
                .post(Entity.json(json)));
    }

    /**
     * Deletes an existing replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void dropReplication(String source, String target) {
        ensureSuccess(resource.path("replications")
                .queryParam("source", source)
                .queryParam("target", target)
                .request()
                .delete());
    }

    /**
     * Add a new Content to a repository. Info is extracted from content an added along.
     *
     * @param repository Repository name.
     * @param filepath Content path (from client perspective).
     * @return Actual command result.
     */
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

    /**
     * Delete an exising content from a repository.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     * @return Actual command result.
     */
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

    /**
     * Provides content info tree for a given content.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     * @return
     */
    public ContentInfoTree getInfoTree(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/info/{hash}")
                .resolveTemplate("name", repository)
                .resolveTemplate("hash", hash)
                .request()
                .get();

        return read(response, ContentInfoTree.class);
    }

    /**
     * Download a content from a repository. Downloaded content is saved in client working directory.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     */
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

    /**
     * Find index entries matching a given query in a paginated way.
     *
     * @param repository Repository name.
     * @param query Query.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of index entries.
     */
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

    /**
     * Find content infos matching a given query in a paginated way.
     *
     * @param repository Repository name.
     * @param query Query.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of content infos.
     */
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

    /**
     * Provides history in a paginated way.
     *
     * @param repository Repository name.
     * @param asc If true, returned list is sorted chronologically.
     * @param from First item to return.
     * @param size Number of items to return.
     * @return A list of history events.
     */
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
