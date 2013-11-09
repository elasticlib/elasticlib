package store.server.resources;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.CREATED;
import store.server.Repository;

@Path("replications")
public class ReplicationsResource {

    private final Repository repository;

    /**
     * Constructor.
     *
     * @param repository The repository.
     */
    public ReplicationsResource(Repository repository) {
        this.repository = repository;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createReplication(JsonObject json) {
        repository.sync(json.getString("source"), json.getString("target"));
        return Response.status(CREATED).build();
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropReplication(JsonObject json) {
        repository.unsync(json.getString("source"), json.getString("target"));
        return Response.status(CREATED).build();
    }

    @GET
    public JsonArray listReplications() {
        return Json.createArrayBuilder().build(); // TODO this is a stub
    }
}
