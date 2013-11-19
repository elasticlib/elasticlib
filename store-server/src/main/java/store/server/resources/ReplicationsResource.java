package store.server.resources;

import java.net.URI;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import static store.common.JsonUtil.hasStringValue;
import store.server.Repository;
import store.server.exception.BadRequestException;

/**
 * Replications REST resource.
 */
@Path("replications")
public class ReplicationsResource {

    @Inject
    private Repository repository;
    @Context
    private UriInfo uriInfo;

    /**
     * Create a new replication.
     * <p>
     * Input:<br>
     * - source (String): Source volume name.<br>
     * - target (String): Target volume name.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Source or target volume was not found.<br>
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @PUT
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
     * - source (String): Source volume name.<br>
     * - target (String): Target volume name.
     * <p>
     * Response:<br>
     * - 201 CREATED: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid query parameters.<br>
     * - 404 NOT FOUND: Source or target volume was not found.<br>
     *
     * @param source source volume name
     * @param target target volume name.
     * @return HTTP response
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    public Response dropReplication(@QueryParam("source") String source, @QueryParam("target") String target) {
        // TODO check that query parameters have actually been supplied.
        repository.unsync(source, target);
        return Response.ok().build();
    }

    @GET
    public JsonArray listReplications() {
        return Json.createArrayBuilder().build(); // TODO this is a stub
    }
}
