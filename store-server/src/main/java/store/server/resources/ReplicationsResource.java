package store.server.resources;

import java.net.URI;
import java.util.List;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import store.common.exception.BadRequestException;
import static store.common.json.JsonValidation.hasStringValue;
import store.common.model.ReplicationInfo;
import store.server.service.ReplicationsService;

/**
 * Replications REST resource.
 */
@Path("replications")
public class ReplicationsResource {

    private static final String ACTION = "action";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    @Inject
    private ReplicationsService replicationsService;
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
        replicationsService.createReplication(source, target);
        URI location = uriInfo.getAbsolutePathBuilder()
                .queryParam(SOURCE, source)
                .queryParam(TARGET, target)
                .build();

        return Response.created(location).build();

    }

    private Response startReplication(String source, String target) {
        replicationsService.startReplication(source, target);
        return Response.ok().build();
    }

    private Response stopReplication(String source, String target) {
        replicationsService.stopReplication(source, target);
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
        replicationsService.deleteReplication(source, target);
        return Response.ok().build();
    }

    /**
     * List info about existing replications.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output data
     */
    @GET
    public GenericEntity<List<ReplicationInfo>> listReplications() {
        return new GenericEntity<List<ReplicationInfo>>(replicationsService.listReplicationInfos()) {
        };
    }

    private static BadRequestException newInvalidJsonException() {
        return new BadRequestException("Invalid json data");
    }
}
