package store.server.resources;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import store.common.Hash;
import static store.common.JsonUtil.hasStringValue;
import store.server.RepositoryManager;
import store.server.exception.BadRequestException;
import store.server.exception.UnknownIndexException;

/**
 * Indexes REST resource.
 */
@Path("indexes")
public class IndexesResource {

    @Inject
    private RepositoryManager repository;
    @Context
    private UriInfo uriInfo;

    /**
     * Create a new index.
     * <p>
     * Input:<br>
     * - path (String): Index path on file system. Index name is the last part of this path.<br>
     * - volume (String): Indexed volume name.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Volume was not found.<br>
     * - 412 PRECONDITION FAILED: Index could not be created at supplied path.
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createIndex(JsonObject json) {
        if (!hasStringValue(json, "path") || !hasStringValue(json, "volume")) {
            throw new BadRequestException();
        }
        java.nio.file.Path path = Paths.get(json.getString("path"));
        repository.createIndex(path, json.getString("volume"));
        return Response
                .created(UriBuilder.fromUri(uriInfo.getRequestUri()).fragment(path.getFileName().toString()).build())
                .build();
    }

    /**
     * Create a new index.
     *
     * @see createIndex(JsonObject)
     * @param name index name
     * @param json input JSON data
     * @return HTTP response
     */
    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createIndex(@PathParam("name") String name, JsonObject json) {
        if (!hasStringValue(json, "path") || !hasStringValue(json, "volume")) {
            throw new BadRequestException();
        }
        repository.createIndex(Paths.get(json.getString("path")).resolve(name), json.getString("volume"));
        return Response.created(uriInfo.getRequestUri()).build();
    }

    /**
     * Delete an index.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: index was not found.
     *
     * @param name index name
     * @return HTTP response
     */
    @DELETE
    @Path("{name}")
    public Response dropIndex(@PathParam("name") String name) {
        repository.dropIndex(name);
        return Response.ok().build();
    }

    /**
     * Update an index.
     *
     * @param name index name
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateIndex(@PathParam("name") String name, JsonObject json) {
        // Le status d'un index est immutable (pour l'instant).
        return Response.status(NOT_IMPLEMENTED).build();
    }

    /**
     * List existing indexes.
     * <p>
     * Output:<br>
     * - Array of index names.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output JSON data
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listIndexes() {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (java.nio.file.Path path : repository.config().getIndexes()) {
            builder.add(path.getFileName().toString());
        }
        return builder.build();
    }

    /**
     * Get info about an index.
     * <p>
     * Output:<br>
     * - name (String): Index name.<br>
     * - path (String): Index path on file system.<br>
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Index was not found.
     *
     * @param name index name
     * @return output JSON data
     */
    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getIndex(@PathParam("name") String name) {
        for (java.nio.file.Path path : repository.config().getVolumes()) {
            if (path.getFileName().toString().equals(name)) {
                return Json.createObjectBuilder()
                        .add("name", name)
                        .add("path", path.toString())
                        .build();
            }
        }
        throw new UnknownIndexException();
    }

    @POST
    @Path("{name}/docs")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response postDocument(@PathParam("name") String name, InputStream entity) {
        // TODO this is a stub
        return Response.status(NOT_IMPLEMENTED).build();
    }

    @PUT
    @Path("{name}/docs/{hash}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putDocument(@PathParam("name") String name, @PathParam("hash") Hash hash, InputStream entity) {
        // TODO this is a stub
        return Response.status(NOT_IMPLEMENTED).build();
    }

    @DELETE
    @Path("{name}/docs/{hash}")
    public Response deleteDocument(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        // TODO this is a stub
        return Response.status(NOT_IMPLEMENTED).build();
    }

    @HEAD
    @Path("{name}/docs/{hash}")
    public Response containsDocument(@PathParam("name") String name, @PathParam("hash") Hash hash) {
        // TODO this is a stub
        return Response.status(NOT_IMPLEMENTED).build();
    }

    /**
     * Find indexed contents matching supplied query.
     * <p>
     * Output:<br>
     * - Array of content hashes.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Index was not found.
     *
     * @param name index name
     * @param query Query
     * @return output JSON data
     */
    @GET
    @Path("{name}/docs")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Hash> find(@PathParam("name") String name, @QueryParam("q") String query) {
        return repository.find(name, query);
    }
}
