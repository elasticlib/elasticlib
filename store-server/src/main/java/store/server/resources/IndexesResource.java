package store.server.resources;

import java.nio.file.Paths;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.CREATED;
import store.common.Hash;
import store.server.Repository;

@Path("indexes")
public class IndexesResource {

    private final Repository repository;

    /**
     * Constructor.
     *
     * @param repository The repository.
     */
    public IndexesResource(Repository repository) {
        this.repository = repository;
    }

    @PUT
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createIndex(@PathParam("name") String name, JsonObject json) {
        repository.createIndex(Paths.get(json.getString("path")).resolve(name), json.getString("volume"));
        return Response.status(CREATED).build();
    }

    @DELETE
    @Path("{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropIndex(@PathParam("name") String name) {
        repository.dropIndex(name);
        return Response.ok().build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listIndexes() {
        return Json.createArrayBuilder().build(); // TODO this is a stub
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Hash> find(@PathParam("name") String name, @QueryParam("q") String query) {
        return repository.find(name, query);
    }
}
