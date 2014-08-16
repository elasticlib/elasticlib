package store.server.resources;

import java.util.ArrayList;
import static java.util.Collections.singletonList;
import java.util.List;
import javax.inject.Inject;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import static store.common.json.JsonValidation.hasArrayValue;
import static store.common.json.JsonValidation.hasStringValue;
import static store.common.json.JsonWriting.writeAll;
import store.server.exception.BadRequestException;
import store.server.service.RemotesService;

/**
 * Remote nodes REST resource.
 */
@Path("remotes")
public class RemotesResource {

    private static final String HOST = "host";
    private static final String HOSTS = "hosts";

    @Inject
    private RemotesService remotesService;

    /**
     * Create a new remote node.
     * <p>
     * Input:<br>
     * - hosts (String array): Publish hosts of the remote node.<br>
     * or<br>
     * - host (String): same, but with a single host
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 503 SERVICE UNAVAILABLE: Remote node was not reachable.
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addRemote(JsonObject json) {
        remotesService.addRemote(parseAddRemoteRequest(json));
        return Response.ok().build();
    }

    private static List<String> parseAddRemoteRequest(JsonObject json) {
        if (hasStringValue(json, HOST)) {
            return singletonList(json.getString(HOST));
        }
        if (hasArrayValue(json, HOSTS)) {
            List<String> list = new ArrayList<>();
            for (JsonValue value : json.getJsonArray(HOSTS)) {
                if (value.getValueType() != ValueType.STRING) {
                    throw newInvalidJsonException();
                }
                list.add(((JsonString) value).getString());
            }
            return list;
        }
        throw newInvalidJsonException();
    }

    private static BadRequestException newInvalidJsonException() {
        return new BadRequestException("Invalid json data");
    }

    /**
     * Remove a node.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Node was not found.
     *
     * @param nodeKey node name or encoded GUID
     * @return HTTP response
     */
    @DELETE
    @Path("{node}")
    public Response removeRemote(@PathParam("node") String nodeKey) {
        remotesService.removeRemote(nodeKey);
        return Response.ok().build();
    }

    /**
     * List definitions of all remote nodes.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output JSON data
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonArray listRemotes() {
        return writeAll(remotesService.listRemotes());
    }
}
