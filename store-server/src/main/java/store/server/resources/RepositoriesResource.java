package store.server.resources;

import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.ContentInfoTree;
import store.common.IndexEntry;
import static store.common.IoUtil.copy;
import store.common.Operation;
import store.common.hash.Hash;
import static store.common.json.JsonReading.read;
import static store.common.json.JsonValidation.hasStringValue;
import static store.common.json.JsonValidation.isValid;
import static store.common.json.JsonWriting.write;
import static store.common.json.JsonWriting.writeAll;
import store.common.metadata.Properties.Common;
import store.common.value.Value;
import store.common.value.ValueType;
import store.server.exception.BadRequestException;
import store.server.exception.WriteException;
import store.server.multipart.FormDataMultipart;
import store.server.service.RepositoriesService;
import store.server.repository.Repository;

/**
 * Repositories REST resource.
 */
@Path("repositories")
public class RepositoriesResource {

    private static final String PATH = "path";
    private static final String ACTION = "action";
    private static final String REPOSITORY = "repository";
    private static final String HASH = "hash";
    private static final String REV = "rev";
    private static final String TX_ID = "txId";
    private static final String HEAD = "head";
    private static final String CONTENT = "content";
    private static final String QUERY = "query";
    private static final String SORT = "sort";
    private static final String FROM = "from";
    private static final String SIZE = "size";
    private static final String ASC = "asc";
    private static final String DESC = "desc";
    private static final String DEFAULT_FROM = "0";
    private static final String DEFAULT_SIZE = "20";
    @Inject
    private RepositoriesService repositoriesService;
    @Context
    private UriInfo uriInfo;

    /**
     * Create a new repository.
     * <p>
     * Input:<br>
     * - path (String): Repository path on file system. Repository name is the last part of this path.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 412 PRECONDITION FAILED: Repository could not be created at supplied path.
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postRepository(JsonObject json) {
        Action action = Action.of(json);
        switch (action) {
            case CREATE:
                return createRepository(json);
            case ADD:
                return addRepository(json);
            case OPEN:
                return openRepository(json);
            case CLOSE:
                return closeRepository(json);
            case REMOVE:
                return removeRepository(json);
            case DELETE:
                return deleteRepository(json);
            default:
                throw new AssertionError();
        }
    }

    private static enum Action {

        CREATE, ADD, OPEN, CLOSE, REMOVE, DELETE;

        public static Action of(JsonObject json) {
            if (!hasStringValue(json, ACTION)) {
                return CREATE;
            }
            String raw = json.getString(ACTION).toUpperCase();
            for (Action action : values()) {
                if (action.name().equals(raw)) {
                    return action;
                }
            }
            throw newInvalidJsonException();
        }
    }

    private Response createRepository(JsonObject json) {
        if (!hasStringValue(json, PATH)) {
            throw newInvalidJsonException();
        }
        java.nio.file.Path path = Paths.get(json.getString(PATH));
        repositoriesService.createRepository(path);
        return Response
                .created(uriInfo.getAbsolutePathBuilder().path(path.getFileName().toString()).build())
                .build();
    }

    private Response addRepository(JsonObject json) {
        if (!hasStringValue(json, PATH)) {
            throw newInvalidJsonException();
        }
        java.nio.file.Path path = Paths.get(json.getString(PATH));
        repositoriesService.addRepository(path);
        return Response
                .created(uriInfo.getAbsolutePathBuilder().path(path.getFileName().toString()).build())
                .build();
    }

    private Response openRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        repositoriesService.openRepository(json.getString(REPOSITORY));
        return Response.ok().build();
    }

    private Response closeRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        repositoriesService.closeRepository(json.getString(REPOSITORY));
        return Response.ok().build();
    }

    private Response removeRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        repositoriesService.removeRepository(json.getString(REPOSITORY));
        return Response.ok().build();
    }

    private Response deleteRepository(JsonObject json) {
        if (!hasStringValue(json, REPOSITORY)) {
            throw newInvalidJsonException();
        }
        return deleteRepository(json.getString(REPOSITORY));
    }

    /**
     * Delete a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @return HTTP response
     */
    @DELETE
    @Path("{repository}")
    public Response deleteRepository(@PathParam(REPOSITORY) String repositoryKey) {
        repositoriesService.deleteRepository(repositoryKey);
        return Response.ok().build();
    }

    /**
     * List info about existing repositoryies.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output JSON data
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listRepositories() {
        return writeAll(repositoriesService.listRepositoryInfos());
    }

    /**
     * Get info about a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @return output JSON data
     */
    @GET
    @Path("{repository}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getRepository(@PathParam(REPOSITORY) String repositoryKey) {
        return write(repositoriesService.getRepositoryInfo(repositoryKey));
    }

    /**
     * Add content info. If associated content is not present, started transaction is suspended so that client may
     * create this content in a latter request.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 202 ACCEPTED: Requester is expected to supply content in a latter request<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 409 CONFLICT: Supplied rev spec did not match existing one.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{repository}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postInfo(@PathParam(REPOSITORY) String repositoryKey, JsonObject json) {
        if (isValid(json, ContentInfo.class)) {
            return response(repository(repositoryKey).addContentInfo(read(json, ContentInfo.class)));
        }
        if (isValid(json, ContentInfoTree.class)) {
            return response(repository(repositoryKey).mergeContentInfoTree(read(json, ContentInfoTree.class)));
        }
        throw newInvalidJsonException();
    }

    /**
     * Resume a previously suspended transaction and create a content.
     * <p>
     * Query param:<br>
     * -txId: identifier of a previously suspended transaction.
     * <p>
     * Input:<br>
     * - content (Raw): Content data.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid form data.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 412 PRECONDITION FAILED: Integrity checking failed.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param transactionId transaction identifier
     * @param formData entity form data
     * @return HTTP response
     */
    @PUT
    @Path("{repository}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response addContent(@PathParam(REPOSITORY) String repositoryKey,
                               @PathParam(HASH) Hash hash,
                               @QueryParam(TX_ID) long transactionId,
                               FormDataMultipart formData) {

        try (InputStream inputStream = formData.next(CONTENT).getAsInputStream()) {
            return response(uriInfo.getAbsolutePath(),
                            repository(repositoryKey).addContent(transactionId, hash, inputStream));

        } catch (IOException e) {
            throw new WriteException(e);
        }
    }

    /**
     * Delete a content.
     * <p>
     * Query param:<br>
     * - rev: specify expected head to apply request on. May be set to "any" if requester makes to expectation about
     * existing head or to a dash-separated sequence of revision hashes of expected existing head. Default to "any".
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 409 CONFLICT: Supplied rev spec did not match existing one.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param rev expected head to apply request on
     * @param hash content hash
     * @return HTTP response
     */
    @DELETE
    @Path("{repository}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response deleteContent(@PathParam(REPOSITORY) String repositoryKey,
                                  @QueryParam(REV) @DefaultValue("") String rev,
                                  @PathParam(HASH) Hash hash) {

        return response(repository(repositoryKey).deleteContent(hash, new TreeSet<>(parseRevisions(rev))));
    }

    private static Response response(CommandResult result) {
        return Response.ok().entity(write(result)).build();
    }

    private static Response response(URI uri, CommandResult result) {
        ResponseBuilder builder;
        if (!result.isNoOp() && result.getOperation() == Operation.CREATE) {
            builder = Response.created(uri);
        } else {
            builder = Response.ok();
        }
        return builder.entity(write(result)).build();
    }

    /**
     * Get a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @return HTTP response
     */
    @GET
    @Path("{repository}/content/{hash}")
    public Response getContent(@PathParam(REPOSITORY) final String repositoryKey, @PathParam(HASH) final Hash hash) {
        while (true) {
            final Repository repository = repository(repositoryKey);
            ResponseBuilder response = Response.ok(new StreamingOutput() {
                @Override
                public void write(OutputStream outputStream) throws IOException {
                    try (InputStream inputStream = repository.getContent(hash)) {
                        copy(inputStream, outputStream);
                    }
                }
            });
            Map<String, Value> metadata = metadata(repository, hash);
            String contentType = value(metadata, Common.CONTENT_TYPE.key());
            if (!contentType.isEmpty()) {
                response.type(contentType);
            }
            String fileName = value(metadata, Common.FILE_NAME.key());
            if (!fileName.isEmpty()) {
                response.header("Content-Disposition", "attachment; filename=" + fileName);
            }
            return response.build();
        }
    }

    private static Map<String, Value> metadata(Repository repository, Hash hash) {
        for (ContentInfo info : repository.getContentInfoHead(hash)) {
            if (!info.isDeleted()) {
                return info.getMetadata();
            }
        }
        return emptyMap();
    }

    private static String value(Map<String, Value> metadata, String key) {
        Value value = metadata.get(key);
        if (value == null || value.type() != ValueType.STRING) {
            return "";
        }
        return value.asString();
    }

    /**
     * Get info about a content.
     * <p>
     * Query param:<br>
     * - rev: specify revisions to returns. May be set to "head" to return current head revisions or to a dash-separated
     * sequence of wanted revision hashes. If unspecified, the whole revision tree is returned.
     *
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository or content was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param rev requested revisions
     * @return output JSON data
     */
    @GET
    @Path("{repository}/info/{hash}")
    public JsonStructure getInfo(@PathParam(REPOSITORY) String repositoryKey,
                                 @PathParam(HASH) Hash hash,
                                 @QueryParam(REV) @DefaultValue("") String rev) {

        if (rev.isEmpty()) {
            return write(repository(repositoryKey).getContentInfoTree(hash));
        }
        if (rev.equals(HEAD)) {
            return writeAll(repository(repositoryKey).getContentInfoHead(hash));
        }
        return writeAll(repository(repositoryKey).getContentInfoRevisions(hash, parseRevisions(rev)));
    }

    private static List<Hash> parseRevisions(String arg) {
        List<Hash> revisions = new ArrayList<>();
        for (String part : Splitter.on('-').split(arg)) {
            revisions.add(new Hash(part));
        }
        return revisions;
    }

    /**
     * Get repository history.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid query parameters.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param sort chronological sorting. Allowed values are "asc" and "desc".
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{repository}/history")
    public JsonArray history(@PathParam(REPOSITORY) String repositoryKey,
                             @QueryParam(SORT) @DefaultValue(DESC) String sort,
                             @QueryParam(FROM) Long from,
                             @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        if (!sort.equals(ASC) && !sort.equals(DESC)) {
            throw newInvalidJsonException();
        }
        if (from == null) {
            from = sort.equals(ASC) ? 0 : Long.MAX_VALUE;
        }
        return writeAll(repository(repositoryKey).history(sort.equals(ASC), from, size));
    }

    /**
     * Find indexed hashes matching supplied query.
     * <p>
     * Output:<br>
     * - Array of content hashes.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{repository}/index")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray find(@PathParam(REPOSITORY) String repositoryKey,
                          @QueryParam(QUERY) String query,
                          @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                          @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        return writeAll(repository(repositoryKey).find(query, from, size));
    }

    /**
     * Find indexed content infos matching supplied query.
     * <p>
     * Output:<br>
     * - Array of content infos.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{repository}/info")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray findInfo(@PathParam(REPOSITORY) String repositoryKey,
                              @QueryParam(QUERY) String query,
                              @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                              @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        List<ContentInfo> infos = new ArrayList<>(size);
        Repository repository = repository(repositoryKey);
        for (IndexEntry entry : repository.find(query, from, size)) {
            infos.addAll(repository.getContentInfoRevisions(entry.getHash(), entry.getRevisions()));
        }
        return writeAll(infos);
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private static BadRequestException newInvalidJsonException() {
        return new BadRequestException("Invalid json data");
    }
}
