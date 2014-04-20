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
import javax.json.Json;
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
import static store.common.json.JsonValidation.hasBooleanValue;
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
import store.server.service.Repository;
import store.server.service.Status;

/**
 * Repositories REST resource.
 */
@Path("repositories")
public class RepositoriesResource {

    private static final String PATH = "path";
    private static final String STARTED = "started";
    private static final String NAME = "name";
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
    public Response createRepository(JsonObject json) {
        if (!hasStringValue(json, PATH)) {
            throw new BadRequestException();
        }
        java.nio.file.Path path = Paths.get(json.getString(PATH));
        repositoriesService.createRepository(path);
        return Response
                .created(uriInfo.getAbsolutePathBuilder().path(path.getFileName().toString()).build())
                .build();
    }

    /**
     * Create a new repository.
     *
     * @see createRepository(JsonObject)
     * @param name repository name
     * @param json input JSON data
     * @return HTTP response
     */
    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createRepository(@PathParam(NAME) String name, JsonObject json) {
        if (!hasStringValue(json, PATH)) {
            throw new BadRequestException();
        }
        repositoriesService.createRepository(Paths.get(json.getString(PATH)).resolve(name));
        return Response.created(uriInfo.getRequestUri()).build();
    }

    /**
     * Delete a repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param name repository name
     * @return HTTP response
     */
    @DELETE
    @Path("{name}")
    public Response dropRepository(@PathParam(NAME) String name) {
        repositoriesService.dropRepository(name);
        return Response.ok().build();
    }

    /**
     * Update a repository.
     * <p>
     * Input:<br>
     * - started (Boolean): Starts/stops the repository.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param name repository name
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateRepository(@PathParam(NAME) String name, JsonObject json) {
        if (!hasBooleanValue(json, STARTED)) {
            throw new BadRequestException();
        }
        if (json.getBoolean(STARTED)) {
            repository(name).start();
        } else {
            repository(name).stop();
        }
        return Response.ok().build();
    }

    /**
     * List existing repositoryies.
     * <p>
     * Output:<br>
     * - Array of repository names.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output JSON data
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listRepositories() {
        return writeAll(repositoriesService.listRepositoryDefs());
    }

    /**
     * Get info about a repository.
     * <p>
     * Output:<br>
     * - name (String): Repository name.<br>
     * - path (String): Repository path on file system.<br>
     * - started (Boolean): If repository is started.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Repository was not found.
     *
     * @param name repository name
     * @return output JSON data
     */
    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getRepository(@PathParam(NAME) String name) {
        Status status = repository(name).getStatus();
        return Json.createObjectBuilder()
                .add(NAME, name)
                .add(PATH, status.getPath().toString())
                .add(STARTED, status.isStarted())
                .build();
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
     * @param name repository name
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{name}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postInfo(@PathParam(NAME) String name, JsonObject json) {
        if (isValid(json, ContentInfo.class)) {
            return response(repository(name).addInfo(read(json, ContentInfo.class)));
        }
        if (isValid(json, ContentInfoTree.class)) {
            return response(repository(name).mergeTree(read(json, ContentInfoTree.class)));
        }
        throw new BadRequestException();
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
     * @param name repository name
     * @param hash content hash
     * @param transactionId transaction identifier
     * @param formData entity form data
     * @return HTTP response
     */
    @PUT
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response addContent(@PathParam(NAME) String name,
                               @PathParam(HASH) Hash hash,
                               @QueryParam(TX_ID) long transactionId,
                               FormDataMultipart formData) {

        try (InputStream inputStream = formData.next(CONTENT).getAsInputStream()) {
            return response(uriInfo.getAbsolutePath(), repository(name).addContent(transactionId, hash, inputStream));

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
     * @param name repository name
     * @param rev expected head to apply request on
     * @param hash content hash
     * @return HTTP response
     */
    @DELETE
    @Path("{name}/content/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response deleteContent(@PathParam(NAME) String name,
                                  @QueryParam(REV) @DefaultValue("") String rev,
                                  @PathParam(HASH) Hash hash) {

        return response(repository(name).deleteContent(hash, new TreeSet<>(parseRevisions(rev))));
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
     * @param name repository name
     * @param hash content hash
     * @return HTTP response
     */
    @GET
    @Path("{name}/content/{hash}")
    public Response getContent(@PathParam(NAME) final String name, @PathParam(HASH) final Hash hash) {
        while (true) {
            final Repository repository = repository(name);
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
        for (ContentInfo info : repository.getInfoHead(hash)) {
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
     * @param name repository name
     * @param hash content hash
     * @param rev requested revisions
     * @return output JSON data
     */
    @GET
    @Path("{name}/info/{hash}")
    public JsonStructure getInfo(@PathParam(NAME) String name,
                                 @PathParam(HASH) Hash hash,
                                 @QueryParam(REV) @DefaultValue("") String rev) {

        if (rev.isEmpty()) {
            return write(repository(name).getInfoTree(hash));
        }
        if (rev.equals(HEAD)) {
            return writeAll(repository(name).getInfoHead(hash));
        }
        return writeAll(repository(name).getInfoRevisions(hash, parseRevisions(rev)));
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
     * @param name repository name
     * @param sort chronological sorting. Allowed values are "asc" and "desc".
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{name}/history")
    public JsonArray history(@PathParam(NAME) String name,
                             @QueryParam(SORT) @DefaultValue(DESC) String sort,
                             @QueryParam(FROM) Long from,
                             @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        if (!sort.equals(ASC) && !sort.equals(DESC)) {
            throw new BadRequestException();
        }
        if (from == null) {
            from = sort.equals(ASC) ? 0 : Long.MAX_VALUE;
        }
        return writeAll(repository(name).history(sort.equals(ASC), from, size));
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
     * @param name repository name
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{name}/index")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray find(@PathParam(NAME) String name,
                          @QueryParam(QUERY) String query,
                          @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                          @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        return writeAll(repository(name).find(query, from, size));
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
     * @param name repository name
     * @param query Query
     * @param from sequence value to start with.
     * @param size number of results to return.
     * @return output JSON data
     */
    @GET
    @Path("{name}/info")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray findInfo(@PathParam(NAME) String name,
                              @QueryParam(QUERY) String query,
                              @QueryParam(FROM) @DefaultValue(DEFAULT_FROM) int from,
                              @QueryParam(SIZE) @DefaultValue(DEFAULT_SIZE) int size) {
        List<ContentInfo> infos = new ArrayList<>(size);
        Repository repository = repository(name);
        for (IndexEntry entry : repository.find(query, from, size)) {
            infos.addAll(repository.getInfoRevisions(entry.getHash(), entry.getRevisions()));
        }
        return writeAll(infos);
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }
}
