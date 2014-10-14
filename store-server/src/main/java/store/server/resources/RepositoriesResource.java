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
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import store.common.exception.BadRequestException;
import store.common.exception.IOFailureException;
import store.common.hash.Guid;
import store.common.hash.Hash;
import static store.common.json.JsonReading.read;
import static store.common.json.JsonValidation.hasStringValue;
import static store.common.json.JsonValidation.isValid;
import store.common.metadata.Properties.Common;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.Event;
import store.common.model.IndexEntry;
import store.common.model.Operation;
import store.common.model.RepositoryInfo;
import store.common.model.Revision;
import store.common.model.RevisionTree;
import store.common.model.StagingInfo;
import static store.common.util.IoUtil.copy;
import store.common.value.Value;
import store.common.value.ValueType;
import store.server.multipart.FormDataMultipart;
import store.server.repository.Repository;
import store.server.service.RepositoriesService;

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
    private static final String SESSION_ID = "sessionId";
    private static final String POSITION = "position";
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
     * Alters state of a repository.
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
     * @return output data
     */
    @GET
    public GenericEntity<List<RepositoryInfo>> listRepositories() {
        return new GenericEntity<List<RepositoryInfo>>(repositoriesService.listRepositoryInfos()) {
        };
    }

    /**
     * Get info about a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @return output data
     */
    @GET
    @Path("{repository}")
    public RepositoryInfo getRepository(@PathParam(REPOSITORY) String repositoryKey) {
        return repositoriesService.getRepositoryInfo(repositoryKey);
    }

    /**
     * Prepares to add a new content in a repository.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @return Info about the staging session created.
     */
    @POST
    @Path("{repository}/stage/{hash}")
    public StagingInfo stageContent(@PathParam(REPOSITORY) String repositoryKey, @PathParam(HASH) Hash hash) {
        return repository(repositoryKey).stageContent(hash);
    }

    /**
     * Writes bytes to a staged content.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @param sessionId Staging session identifier
     * @param position Position in staged content at which write should begin
     * @param formData entity form data
     * @return Updated info of the staging session.
     */
    @POST
    @Path("{repository}/stage/{hash}/{sessionId}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public StagingInfo writeContent(@PathParam(REPOSITORY) String repositoryKey,
                                    @PathParam(HASH) Hash hash,
                                    @PathParam(SESSION_ID) Guid sessionId,
                                    @QueryParam(POSITION) long position,
                                    FormDataMultipart formData) {

        try (InputStream inputStream = formData.next(CONTENT).getAsInputStream()) {
            return repository(repositoryKey).writeContent(hash, sessionId, inputStream, position);

        } catch (IOException e) {
            throw new IOFailureException(e);
        }
    }

    /**
     * Add a revision or a revision tree. If associated content is not present, started transaction is suspended so that
     * client may create this content in a latter request.
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
    @Path("{repository}/revisions")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addRevision(@PathParam(REPOSITORY) String repositoryKey, JsonObject json) {
        if (isValid(json, Revision.class)) {
            return response(repository(repositoryKey).addRevision(read(json, Revision.class)));
        }
        if (isValid(json, RevisionTree.class)) {
            return response(repository(repositoryKey).mergeTree(read(json, RevisionTree.class)));
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
    @Path("{repository}/contents/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response addContent(@PathParam(REPOSITORY) String repositoryKey,
                               @PathParam(HASH) Hash hash,
                               @QueryParam(TX_ID) long transactionId,
                               FormDataMultipart formData) {

        try (InputStream inputStream = formData.next(CONTENT).getAsInputStream()) {
            return response(uriInfo.getAbsolutePath(),
                            repository(repositoryKey).addContent(transactionId, hash, inputStream));

        } catch (IOException e) {
            throw new IOFailureException(e);
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
    @Path("{repository}/contents/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response deleteContent(@PathParam(REPOSITORY) String repositoryKey,
                                  @QueryParam(REV) @DefaultValue("") String rev,
                                  @PathParam(HASH) Hash hash) {

        return response(repository(repositoryKey).deleteContent(hash, new TreeSet<>(parseRevisions(rev))));
    }

    private static Response response(CommandResult result) {
        return Response.ok()
                .entity(result)
                .build();
    }

    private static Response response(URI uri, CommandResult result) {
        ResponseBuilder builder;
        if (!result.isNoOp() && result.getOperation() == Operation.CREATE) {
            builder = Response.created(uri);
        } else {
            builder = Response.ok();
        }
        return builder.entity(result).build();
    }

    /**
     * Get info about a content.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.<br>
     * - 503 SERVICE UNAVAILABLE: Repository is not started.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param hash content hash
     * @return Corresponding content info.
     */
    @GET
    @Path("{repository}/info/{hash}")
    public ContentInfo getContentInfo(@PathParam(REPOSITORY) String repositoryKey, @PathParam(HASH) Hash hash) {
        return repository(repositoryKey).getContentInfo(hash);
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
    @Path("{repository}/contents/{hash}")
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
        for (Revision rev : repository.getHead(hash)) {
            if (!rev.isDeleted()) {
                return rev.getMetadata();
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
     * Get revisions about a content.
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
     * @return HTTP response
     */
    @GET
    @Path("{repository}/revisions/{hash}")
    public Response getRevisions(@PathParam(REPOSITORY) String repositoryKey,
                                 @PathParam(HASH) Hash hash,
                                 @QueryParam(REV) @DefaultValue("") String rev) {

        if (rev.isEmpty()) {
            return Response.ok()
                    .entity(repository(repositoryKey).getTree(hash))
                    .build();
        }
        if (rev.equals(HEAD)) {
            return response(repository(repositoryKey).getHead(hash));
        }
        return response(repository(repositoryKey).getRevisions(hash, parseRevisions(rev)));
    }

    private static List<Hash> parseRevisions(String arg) {
        List<Hash> revisions = new ArrayList<>();
        for (String part : Splitter.on('-').split(arg)) {
            revisions.add(new Hash(part));
        }
        return revisions;
    }

    private static Response response(List<Revision> contentInfos) {
        GenericEntity<List<Revision>> entity = new GenericEntity<List<Revision>>(contentInfos) {
        };
        return Response.ok()
                .entity(entity)
                .build();
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
     * @return output data
     */
    @GET
    @Path("{repository}/history")
    public GenericEntity<List<Event>> history(@PathParam(REPOSITORY) String repositoryKey,
                                              @QueryParam(SORT) @DefaultValue(DESC) String sort,
                                              @QueryParam(FROM) Long from,
                                              @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        if (!sort.equals(ASC) && !sort.equals(DESC)) {
            throw newInvalidJsonException();
        }
        if (from == null) {
            from = sort.equals(ASC) ? 0 : Long.MAX_VALUE;
        }
        List<Event> events = repository(repositoryKey).history(sort.equals(ASC), from, size);
        return new GenericEntity<List<Event>>(events) {
        };
    }

    /**
     * Find index entries matching supplied query.
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
     * @return output data
     */
    @GET
    @Path("{repository}/index")
    public GenericEntity<List<IndexEntry>> find(@PathParam(REPOSITORY) String repositoryKey,
                                                @QueryParam(QUERY) String query,
                                                @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                                                @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {

        List<IndexEntry> entries = repository(repositoryKey).find(query, from, size);
        return new GenericEntity<List<IndexEntry>>(entries) {
        };
    }

    /**
     * Find indexed revisions matching supplied query.
     * <p>
     * Output:<br>
     * - Array of revisions.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param repositoryKey repository name or encoded GUID
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output data
     */
    @GET
    @Path("{repository}/revisions")
    public GenericEntity<List<Revision>> findRevisions(@PathParam(REPOSITORY) String repositoryKey,
                                                       @QueryParam(QUERY) String query,
                                                       @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                                                       @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        List<Revision> infos = new ArrayList<>(size);
        Repository repository = repository(repositoryKey);
        for (IndexEntry entry : repository.find(query, from, size)) {
            infos.addAll(repository.getRevisions(entry.getHash(), entry.getRevisions()));
        }
        return new GenericEntity<List<Revision>>(infos) {
        };
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }

    private static BadRequestException newInvalidJsonException() {
        return new BadRequestException("Invalid json data");
    }
}
