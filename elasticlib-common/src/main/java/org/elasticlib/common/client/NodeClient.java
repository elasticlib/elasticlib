package org.elasticlib.common.client;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static org.elasticlib.common.client.ClientUtil.read;
import org.elasticlib.common.model.NodeDef;

/**
 * Node API client.
 */
public class NodeClient {

    private final WebTarget resource;

    /**
     * Constructor.
     *
     * @param resource Base web-resource.
     */
    NodeClient(WebTarget resource) {
        this.resource = resource;
    }

    /**
     * Provides the definition of the node this client is currently connected to.
     *
     * @return A node definition.
     */
    public NodeDef getDef() {
        Response response = resource.request().get();
        return read(response, NodeDef.class);
    }
}
