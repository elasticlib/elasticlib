package store.server.resources;

import java.net.URI;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import store.common.Config;
import static store.common.json.JsonUtil.hasStringValue;
import store.server.RepositoryManager;
import store.server.exception.BadRequestException;

/**
 * Replications REST resource.
 */
@Path("replications")
public class ReplicationsResource {

    @Inject
    private RepositoryManager repository;
    @Context
    private UriInfo uriInfo;

    /**
     * Create a new replication.
     * <p>
     * Input:<br>
     * - source (String): Source name.<br>
     * - target (String): Target name.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Source or target was not found.<br>
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createReplication(JsonObject json) {
        if (!hasStringValue(json, "source") || !hasStringValue(json, "target")) {
            throw new BadRequestException();
        }
        String source = json.getString("source");
        String target = json.getString("target");
        repository.sync(source, target);
        URI location = UriBuilder
                .fromUri(uriInfo.getRequestUri())
                .queryParam("source", source)
                .queryParam("target", target)
                .build();

        return Response.created(location).build();
    }

    /**
     * Delete a replication.
     * <p>
     * Input:<br>
     * - source (String): Source name.<br>
     * - target (String): Target name.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid query parameters.<br>
     * - 404 NOT FOUND: Source or target was not found.<br>
     *
     * @param source source name
     * @param target target name.
     * @return HTTP response
     */
    @DELETE
    public Response deleteReplication(@QueryParam("source") String source, @QueryParam("target") String target) {
        if (source == null || target == null) {
            throw new BadRequestException();
        }
        repository.unsync(source, target);
        return Response.ok().build();
    }

    /**
     * List existing replications.
     * <p>
     * Output:<br>
     * - Array of replications.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output JSON data
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listReplications() {
        JsonArrayBuilder builder = Json.createArrayBuilder();
        Config config = repository.config();
        for (java.nio.file.Path path : config.getRepositories()) {
            String source = path.getFileName().toString();
            for (String target : config.getSync(source)) {
                builder.add(Json.createObjectBuilder().add("source", source).add("target", target));
            }
        }
        return builder.build();
    }
}
