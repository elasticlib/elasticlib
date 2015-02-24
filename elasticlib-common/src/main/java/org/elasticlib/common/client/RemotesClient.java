/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.common.client;

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
import static org.elasticlib.common.client.ClientUtil.ensureSuccess;
import static org.elasticlib.common.client.ClientUtil.readAll;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RemoteInfo;

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
    public List<RemoteInfo> listInfos() {
        Response response = resource.request().get();
        return readAll(response, RemoteInfo.class);
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
        uris.forEach(uri -> urisArray.add(uri.toString()));

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
