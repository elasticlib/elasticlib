package org.elasticlib.node.resources;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.node.service.NodesService;

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
