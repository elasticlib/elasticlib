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
import store.server.Repository;
import store.server.exception.UnknownIndexException;

@Path("indexes")
public class IndexesResource {

    @Inject
    private Repository repository;
    @Context
    private UriInfo uriInfo;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createIndex(JsonObject json) {
        java.nio.file.Path path = Paths.get(json.getString("path"));
        repository.createIndex(path, json.getString("volume"));
        return Response
                .created(UriBuilder.fromUri(uriInfo.getRequestUri()).fragment(path.getFileName().toString()).build())
                .build();
    }

    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createIndex(@PathParam("name") String name, JsonObject json) {
        repository.createIndex(Paths.get(json.getString("path")).resolve(name), json.getString("volume"));
        return Response.created(uriInfo.getRequestUri()).build();
    }

    @DELETE
    @Path("{name}")
    public Response dropIndex(@PathParam("name") String name) {
        repository.dropIndex(name);
        return Response.ok().build();
    }

    @POST
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateIndex(@PathParam("name") String name, JsonObject json) {
        // Le status d'un index est immutable (pour l'instant).
        return Response.status(NOT_IMPLEMENTED).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listIndexes() {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        for (java.nio.file.Path path : repository.config().getIndexes()) {
            builder.add(path.getFileName().toString());
        }
        return builder.build();
    }

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

    @GET
    @Path("{name}/docs")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Hash> find(@PathParam("name") String name, @QueryParam("q") String query) {
        return repository.find(name, query);
    }
}
