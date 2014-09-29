package store.server.resources;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import store.common.model.NodeDef;
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
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output data
     */
    @GET
    public NodeDef getNodeDef() {
        return nodesService.getNodeDef();
    }
}
