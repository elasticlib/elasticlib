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

    private static final String ACTION = "action";
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
     * - source (String): Source name or encoded GUID.<br>
     * - target (String): Target name or encoded GUID.
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
    public Response postReplication(JsonObject json) {
        if (!hasStringValue(json, SOURCE) || !hasStringValue(json, TARGET)) {
            throw newInvalidJsonException();
        }
        Action action = Action.of(json);
        String source = json.getString(SOURCE);
        String target = json.getString(TARGET);
        switch (action) {
            case CREATE:
                return createReplication(source, target);
            case START:
                return startReplication(source, target);
            case STOP:
                return stopReplication(source, target);
            case DELETE:
                return deleteReplication(source, target);
            default:
                throw new AssertionError();
        }
    }

    private static enum Action {

        CREATE, START, STOP, DELETE;

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

    private Response createReplication(String source, String target) {
        repositoriesService.createReplication(source, target);
        URI location = uriInfo.getAbsolutePathBuilder()
                .queryParam(SOURCE, source)
                .queryParam(TARGET, target)
                .build();

        return Response.created(location).build();

    }

    private Response startReplication(String source, String target) {
        repositoriesService.startReplication(source, target);
        return Response.ok().build();
    }

    private Response stopReplication(String source, String target) {
        repositoriesService.stopReplication(source, target);
        return Response.ok().build();
    }

    /**
     * Delete a replication.
     * <p>
     * Input:<br>
     * - source (String): Source name or encoded GUID.<br>
     * - target (String): Target name or encoded GUID.
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
        repositoriesService.deleteReplication(source, target);
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

    private static BadRequestException newInvalidJsonException() {
        return new BadRequestException("Invalid json data");
    }
}
