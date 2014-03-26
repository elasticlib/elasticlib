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
import javax.json.JsonArrayBuilder;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.Hash;
import store.common.IndexEntry;
import static store.common.IoUtil.copy;
import store.common.Operation;
import static store.common.json.JsonUtil.hasBooleanValue;
import static store.common.json.JsonUtil.hasStringValue;
import static store.common.json.JsonUtil.isContentInfo;
import static store.common.json.JsonUtil.isContentInfoTree;
import static store.common.json.JsonUtil.readContentInfo;
import static store.common.json.JsonUtil.readContentInfoTree;
import static store.common.json.JsonUtil.writeCommandResult;
import static store.common.json.JsonUtil.writeContentInfoTree;
import static store.common.json.JsonUtil.writeContentInfos;
import static store.common.json.JsonUtil.writeEvents;
import static store.common.json.JsonUtil.writeIndexEntries;
import store.common.value.Value;
import store.common.value.ValueType;
import store.server.exception.BadRequestException;
import store.server.exception.WriteException;
import store.server.multipart.FormDataMultipart;
import store.server.service.RepositoriesService;
import store.server.service.Repository;
import store.server.service.Status;

/**
 * Volumes REST resource.
 */
@Path("repositories")
public class RepositoriesResource {

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
        if (!hasStringValue(json, "path")) {
            throw new BadRequestException();
        }
        java.nio.file.Path path = Paths.get(json.getString("path"));
        repositoriesService.createRepository(path);
        return Response
                .created(UriBuilder.fromUri(uriInfo.getRequestUri()).path(path.getFileName().toString()).build())
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
    public Response createRepository(@PathParam("name") String name, JsonObject json) {
        if (!hasStringValue(json, "path")) {
            throw new BadRequestException();
        }
        repositoriesService.createRepository(Paths.get(json.getString("path")).resolve(name));
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
    public Response dropRepository(@PathParam("name") String name) {
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
    public Response updateRepository(@PathParam("name") String name, JsonObject json) {
        if (!hasBooleanValue(json, "started")) {
            throw new BadRequestException();
        }
        if (json.getBoolean("started")) {
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
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (java.nio.file.Path path : repositoriesService.getConfig().getRepositories()) {
            builder.add(path.getFileName().toString());
        }
        return builder.build();
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
    public JsonObject getRepository(@PathParam("name") String name) {
        Status status = repository(name).getStatus();
        return Json.createObjectBuilder()
                .add("name", name)
                .add("path", status.getPath().toString())
                .add("started", status.isStarted())
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
    public Response postInfo(@PathParam("name") String name, JsonObject json) {
        if (isContentInfo(json)) {
            return response(repository(name).addInfo(readContentInfo(json)));
        }
        if (isContentInfoTree(json)) {
            return response(repository(name).mergeTree(readContentInfoTree(json)));
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
    public Response addContent(@PathParam("name") String name,
                                  @PathParam("hash") Hash hash,
                                  @QueryParam("txId") int transactionId,
                                  FormDataMultipart formData) {

        try (InputStream inputStream = formData.next("content").getAsInputStream()) {
            return response(uriInfo.getRequestUri(), repository(name).addContent(transactionId, hash, inputStream));

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
    public Response deleteContent(@PathParam("name") String name,
                                  @QueryParam("rev") @DefaultValue("") String rev,
                                  @PathParam("hash") Hash hash) {

        return response(repository(name).deleteContent(hash, new TreeSet<>(parseRevisions(rev))));
    }

    private static Response response(CommandResult result) {
        return Response.ok().entity(writeCommandResult(result)).build();
    }

    private static Response response(URI uri, CommandResult result) {
        ResponseBuilder builder;
        if (!result.isNoOp() && result.getOperation() == Operation.CREATE) {
            builder = Response.created(uri);
        } else {
            builder = Response.ok();
        }
        return builder.entity(writeCommandResult(result)).build();
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
    public Response getContent(@PathParam("name") final String name, @PathParam("hash") final Hash hash) {
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
            String contentType = value(metadata, "contentType");
            if (!contentType.isEmpty()) {
                response.type(contentType);
            }
            String fileName = value(metadata, "fileName");
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
    public JsonStructure getInfo(@PathParam("name") String name,
                                 @PathParam("hash") Hash hash,
                                 @QueryParam("rev") @DefaultValue("") String rev) {

        if (rev.isEmpty()) {
            return writeContentInfoTree(repository(name).getInfoTree(hash));
        }
        if (rev.equals("head")) {
            return writeContentInfos(repository(name).getInfoHead(hash));
        }
        return writeContentInfos(repository(name).getInfoRevisions(hash, parseRevisions(rev)));
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
    public JsonArray history(@PathParam("name") String name,
                             @QueryParam("sort") @DefaultValue("desc") String sort,
                             @QueryParam("from") Long from,
                             @QueryParam("size") @DefaultValue("20") int size) {

        if (!sort.equals("asc") && !sort.equals("desc")) {
            throw new BadRequestException();
        }
        if (from == null) {
            from = sort.equals("asc") ? 0 : Long.MAX_VALUE;
        }
        return writeEvents(repository(name).history(sort.equals("asc"), from, size));
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
    public JsonArray find(@PathParam("name") String name,
                          @QueryParam("query") String query,
                          @QueryParam("from") @DefaultValue("0") int from,
                          @QueryParam("size") @DefaultValue("20") int size) {
        return writeIndexEntries(repository(name).find(query, from, size));
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
    public JsonArray findInfo(@PathParam("name") String name,
                              @QueryParam("query") String query,
                              @QueryParam("from") @DefaultValue("0") int from,
                              @QueryParam("size") @DefaultValue("20") int size) {
        List<ContentInfo> infos = new ArrayList<>(size);
        Repository repository = repository(name);
        for (IndexEntry entry : repository.find(query, from, size)) {
            infos.addAll(repository.getInfoRevisions(entry.getHash(), entry.getHead()));
        }
        return writeContentInfos(infos);
    }

    private Repository repository(String name) {
        return repositoriesService.getRepository(name);
    }
}
