package store.server.resources;

import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import static store.common.json.JsonWriting.write;
import store.server.service.NodesService;

/**
 * Root REST resource.
 */
@Path("/")
public class RootResource {

    @Inject
    private NodesService nodesService;

    /**
     * Provides the definition of the local node.
     *
     * @return A NodeDef instance.
     */
    @GET
    public JsonObject getNodeDef() {
        return write(nodesService.getNodeDef());
    }
}
