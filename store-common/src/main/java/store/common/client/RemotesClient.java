package store.common.client;

import java.net.URI;
import static java.util.Collections.singletonList;
import java.util.List;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static store.common.client.ClientUtil.ensureSuccess;
import static store.common.client.ClientUtil.readAll;
import store.common.hash.Guid;
import store.common.model.NodeInfo;

/**
 * Remotes API client.
 */
public class RemotesClient {

    private static final String REMOTES = "remotes";
    private static final String NODE_TEMPLATE = "{node}";
    private static final String URI = "uri";
    private static final String URIS = "uris";
    private static final String NODE = "node";
    private final WebTarget resource;

    /**
     * Constructor.
     *
     * @param resource Base web-resource.
     */
    RemotesClient(WebTarget resource) {
        this.resource = resource.path(REMOTES);
    }

    /**
     * Lists remote nodes.
     *
     * @return A list of node definitions.
     */
    public List<NodeInfo> listInfos() {
        Response response = resource.request().get();
        return readAll(response, NodeInfo.class);
    }

    /**
     * Adds a remote node.
     *
     * @param uri Remote node URI
     */
    public void add(URI uri) {
        add(singletonList(uri));
    }

    /**
     * Adds a remote node. Expects supplied list not to be empty.
     *
     * @param uris Remote node URI(s)
     */
    public void add(List<URI> uris) {
        ensureSuccess(resource
                .request()
                .post(json(addRemoteBody(uris))));
    }

    private static JsonObject addRemoteBody(List<URI> uris) {
        if (uris.isEmpty()) {
            throw new IllegalArgumentException();
        }
        if (uris.size() == 1) {
            return createObjectBuilder()
                    .add(URI, uris.get(0).toString())
                    .build();
        }
        JsonArrayBuilder urisArray = createArrayBuilder();
        for (URI uri : uris) {
            urisArray.add(uri.toString());
        }
        return createObjectBuilder()
                .add(URIS, urisArray)
                .build();
    }

    /**
     * Removes a remote node.
     *
     * @param node Remote node name or encoded GUID.
     */
    public void remove(String node) {
        ensureSuccess(resource.path(NODE_TEMPLATE)
                .resolveTemplate(NODE, node)
                .request()
                .delete());
    }

    /**
     * Removes a remote node.
     *
     * @param guid Remote node guid.
     */
    public void remove(Guid guid) {
        remove(guid.asHexadecimalString());
    }
}
