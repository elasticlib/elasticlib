package store.server.resources;

import java.net.URI;
import javax.inject.Inject;
import javax.json.JsonArray;
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
import javax.ws.rs.core.UriInfo;
import static store.common.json.JsonValidation.hasStringValue;
import static store.common.json.JsonWriting.writeAll;
import store.server.exception.BadRequestException;
import store.server.service.RepositoriesService;

/**
 * Replications REST resource.
 */
@Path("replications")
public class ReplicationsResource {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    @Inject
    private RepositoriesService repositoriesService;
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
        if (!hasStringValue(json, SOURCE) || !hasStringValue(json, TARGET)) {
            throw new BadRequestException();
        }
        String source = json.getString(SOURCE);
        String target = json.getString(TARGET);
        repositoriesService.createReplication(source, target);
        URI location = uriInfo.getAbsolutePathBuilder()
                .queryParam(SOURCE, source)
                .queryParam(TARGET, target)
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
    public Response deleteReplication(@QueryParam(SOURCE) String source, @QueryParam(TARGET) String target) {
        if (source == null || target == null) {
            throw new BadRequestException();
        }
        repositoriesService.dropReplication(source, target);
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
        return writeAll(repositoriesService.listReplicationDefs());
    }
}
