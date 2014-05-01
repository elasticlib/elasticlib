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
import static java.util.Collections.emptyList;
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
import org.glassfish.jersey.apache.connector.ApacheConnectorProvider;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import static org.glassfish.jersey.media.multipart.Boundary.addBoundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import static store.client.util.ClientUtil.isDeleted;
import static store.client.util.ClientUtil.revisions;
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
import store.common.ReplicationDef;
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

    private static final String NAME = "name";
    private static final String PATH = "path";
    private static final String REPOSITORIES = "repositories";
    private static final String REPLICATIONS = "replications";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String CONTENT = "content";
    private static final String HASH = "hash";
    private static final String REV = "rev";
    private static final String HEAD = "head";
    private static final String TX_ID = "txId";
    private static final String CONTENT_DISPOSITION = "Content-Disposition";
    private static final String FILENAME = "filename";
    private static final String QUERY = "query";
    private static final String FROM = "from";
    private static final String SIZE = "size";
    private static final String SORT = "sort";
    private static final String ASC = "asc";
    private static final String DESC = "desc";
    private final Display display;
    private final ClientConfig config;
    private final Client client;
    private final WebTarget resource;
    private final PrintingFilter printingFilter;

    /**
     * Constructor.
     *
     * @param url Server url.
     * @param display Display to use.
     * @param config Config to use.
     */
    public HttpClient(String url, Display display, ClientConfig config) {
        this.display = display;
        this.config = config;
        printingFilter = new PrintingFilter(display, config);

        Logger logger = Logger.getGlobal();
        logger.setLevel(Level.OFF);

        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig()
                .property(ClientProperties.CHUNKED_ENCODING_SIZE, 1024)
                .connectorProvider(new ApacheConnectorProvider())
                .register(MultiPartFeature.class)
                .register(new HeaderRestoringWriterInterceptor())
                .register(new LoggingFilter(logger, true))
                .register(printingFilter);

        client = ClientBuilder.newClient(clientConfig);
        resource = client.target(url);
    }

    /**
     * Set if HTTP dialog should be printed.
     *
     * @param val If this feature should be activated.
     */
    public void printHttpDialog(boolean val) {
        printingFilter.printHttpDialog(val);
    }

    private static void ensureSuccess(Response response) {
        try {
            checkStatus(response);

        } finally {
            response.close();
        }
    }

    private static <T extends Mappable> T read(Response response, Class<T> clazz) {
        try {
            JsonObject json = checkStatus(response).readEntity(JsonObject.class);
            return JsonReading.read(json, clazz);

        } finally {
            response.close();
        }
    }

    private static <T extends Mappable> List<T> readAll(Response response, Class<T> clazz) {
        try {
            JsonArray array = checkStatus(response).readEntity(JsonArray.class);
            return JsonReading.readAll(array, clazz);

        } finally {
            response.close();
        }
    }

    private static CommandResult result(Response response) {
        return read(response, CommandResult.class);
    }

    private static Response checkStatus(Response response) {
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

    /**
     * Tests connection to current server. Fails if connection is down.
     */
    void testConnection() {
        ensureSuccess(resource.request().get());
    }

    /**
     * Tests that supplied repository really exists. Fails if it does not.
     *
     * @param name Repository name.
     */
    void testRepository(String name) {
        ensureSuccess(resource.path("repositories/{name}")
                .resolveTemplate(NAME, name)
                .request()
                .get());
    }

    /**
     * Creates a new repository at supplied path.
     *
     * @param path Repository path (from server perspective).
     */
    public void createRepository(Path path) {
        JsonObject json = createObjectBuilder()
                .add(PATH, path.toAbsolutePath().toString())
                .build();

        ensureSuccess(resource.path(REPOSITORIES)
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
                .resolveTemplate(NAME, name)
                .request()
                .delete());
    }

    /**
     * Lists existing repositories.
     *
     * @return A list of repository definitions.
     */
    public List<RepositoryDef> listRepositoryDefs() {
        Response response = resource.path(REPOSITORIES)
                .request()
                .get();

        return readAll(response, RepositoryDef.class);
    }

    /**
     * Lists existing replications.
     *
     * @return A list of replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs() {
        Response response = resource.path(REPLICATIONS)
                .request()
                .get();

        return readAll(response, ReplicationDef.class);
    }

    /**
     * Creates a new replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void createReplication(String source, String target) {
        JsonObject json = createObjectBuilder()
                .add(SOURCE, source)
                .add(TARGET, target)
                .build();

        ensureSuccess(resource.path(REPLICATIONS)
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
        ensureSuccess(resource.path(REPLICATIONS)
                .queryParam(SOURCE, source)
                .queryParam(TARGET, target)
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
            List<ContentInfo> head = headIfAny(repository, digest.getHash());
            if (!isDeleted(head)) {
                throw new RequestFailedException("This content is already stored");
            }
            ContentInfo info = new ContentInfoBuilder()
                    .withContent(digest.getHash())
                    .withLength(digest.getLength())
                    .withParents(revisions(head))
                    .withMetadata(metadata(filepath))
                    .computeRevisionAndBuild();

            CommandResult firstStepResult = post(repository, info);
            if (firstStepResult.isNoOp() || firstStepResult.getOperation() != Operation.CREATE) {
                return firstStepResult;
            }
            return putContent(repository, firstStepResult.getTransactionId(), info, filepath);

        } catch (IOException e) {
            throw new RequestFailedException(e);
        }
    }

    private Digest digest(Path filepath) throws IOException {
        try (InputStream inputStream = new LoggingInputStream(display,
                                                              config,
                                                              "Computing content digest",
                                                              newInputStream(filepath),
                                                              size(filepath))) {
            return copyAndDigest(inputStream, sink());
        }
    }

    private CommandResult putContent(String repository,
                                     long transactionId,
                                     ContentInfo info,
                                     Path filepath) throws IOException {

        try (InputStream inputStream = new LoggingInputStream(display,
                                                              config,
                                                              "Uploading content",
                                                              newInputStream(filepath),
                                                              info.getLength())) {
            MultiPart multipart = new FormDataMultiPart()
                    .bodyPart(new StreamDataBodyPart(CONTENT,
                                                     inputStream,
                                                     filepath.getFileName().toString(),
                                                     MediaType.APPLICATION_OCTET_STREAM_TYPE));

            return result(resource.path("repositories/{name}/content/{hash}")
                    .resolveTemplate(NAME, repository)
                    .resolveTemplate(HASH, info.getContent())
                    .queryParam(TX_ID, transactionId)
                    .request()
                    .put(entity(multipart, addBoundary(multipart.getMediaType()))));
        }
    }

    private List<ContentInfo> headIfAny(String repository, Hash hash) {
        Response response = head(repository, hash);
        try {
            if (response.getStatusInfo().getStatusCode() == Status.NOT_FOUND.getStatusCode()) {
                return emptyList();
            }
            return readAll(response, ContentInfo.class);

        } finally {
            response.close();
        }
    }

    /**
     * Update an exising content in a repository.
     *
     * @param repository Repository name.
     * @param info New head revision.
     * @return Actual command result.
     */
    public CommandResult update(String repository, ContentInfo info) {
        return post(repository, info);
    }

    private CommandResult post(String repository, ContentInfo info) {
        return result(resource
                .path("repositories/{name}/info")
                .resolveTemplate(NAME, repository)
                .request()
                .post(entity(write(info), MediaType.APPLICATION_JSON_TYPE)));
    }

    /**
     * Delete an exising content from a repository.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     * @return Actual command result.
     */
    public CommandResult delete(String repository, Hash hash) {
        List<ContentInfo> head = getInfoHead(repository, hash);
        if (isDeleted(head)) {
            throw new RequestFailedException("This content is already deleted");
        }
        return result(resource
                .path("repositories/{name}/content/{hash}")
                .resolveTemplate(NAME, repository)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, Joiner.on('-').join(revisions(head)))
                .request()
                .delete());
    }

    /**
     * Provides content info tree for a given content.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     * @return Corresponding info tree.
     */
    public ContentInfoTree getInfoTree(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/info/{hash}")
                .resolveTemplate(NAME, repository)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        return read(response, ContentInfoTree.class);
    }

    /**
     * Provides info head revisions for a given content.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     * @return Corresponding info head.
     */
    public List<ContentInfo> getInfoHead(String repository, Hash hash) {
        return readAll(head(repository, hash), ContentInfo.class);
    }

    private Response head(String repository, Hash hash) {
        return resource.path("repositories/{name}/info/{hash}")
                .resolveTemplate(NAME, repository)
                .resolveTemplate(HASH, hash)
                .queryParam(REV, HEAD)
                .request()
                .get();
    }

    /**
     * Download a content from a repository. Downloaded content is saved in client working directory.
     *
     * @param repository Repository name.
     * @param hash Content hash.
     */
    public void get(String repository, Hash hash) {
        Response response = resource.path("repositories/{name}/content/{hash}")
                .resolveTemplate(NAME, repository)
                .resolveTemplate(HASH, hash)
                .request()
                .get();

        try {
            checkStatus(response);
            Path path = Paths.get(fileName(response).or(hash.asHexadecimalString()));
            try (InputStream inputStream = response.readEntity(InputStream.class);
                    OutputStream outputStream = new DefferedFileOutputStream(path)) {
                copy(inputStream, outputStream);

            } catch (IOException e) {
                throw new RequestFailedException(e);
            }
        } finally {
            response.close();
        }
    }

    private static Optional<String> fileName(Response response) {
        String header = response.getHeaders().getFirst(CONTENT_DISPOSITION).toString();
        if (header == null || header.isEmpty()) {
            return Optional.absent();
        }
        for (String param : Splitter.on(';').trimResults().omitEmptyStrings().split(header)) {
            List<String> parts = Splitter.on('=').trimResults().omitEmptyStrings().splitToList(param);
            if (parts.size() == 2 && parts.get(0).equalsIgnoreCase(FILENAME)) {
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
                .resolveTemplate(NAME, repository)
                .queryParam(QUERY, query)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
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
                .resolveTemplate(NAME, repository)
                .queryParam(QUERY, query)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
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
                .resolveTemplate(NAME, repository)
                .queryParam(SORT, asc ? ASC : DESC)
                .queryParam(FROM, from)
                .queryParam(SIZE, size)
                .request()
                .get();

        return readAll(response, Event.class);
    }

    @Override
    public void close() {
        client.close();
    }
}
