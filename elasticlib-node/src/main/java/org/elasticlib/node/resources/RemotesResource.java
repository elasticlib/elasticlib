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
package org.elasticlib.node.resources;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import java.util.List;
import javax.inject.Inject;
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
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.elasticlib.common.exception.BadRequestException;
import static org.elasticlib.common.json.JsonValidation.hasArrayValue;
import static org.elasticlib.common.json.JsonValidation.hasStringValue;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.node.service.RemotesService;

/**
 * Remote nodes REST resource.
 */
@Path("remotes")
public class RemotesResource {

    private static final String URI = "uri";
    private static final String URIS = "uris";
    @Inject
    private RemotesService remotesService;

    /**
     * Create a new remote node.
     * <p>
     * Input:<br>
     * - uris (String array): base URIs of the remote node.<br>
     * or<br>
     * - uri (String): same, but with a single URI
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

    private static List<URI> parseAddRemoteRequest(JsonObject json) {
        if (hasStringValue(json, URI)) {
            return singletonList(asUri(json.getString(URI)));
        }
        if (hasArrayValue(json, URIS)) {
            List<URI> list = new ArrayList<>();
            for (JsonValue value : json.getJsonArray(URIS)) {
                if (value.getValueType() != ValueType.STRING) {
                    throw newInvalidJsonException();
                }
                list.add(asUri(((JsonString) value).getString()));
            }
            return list;
        }
        throw newInvalidJsonException();
    }

    private static URI asUri(String value) {
        try {
            return new URI(value);

        } catch (URISyntaxException e) {
            throw new BadRequestException(e);
        }
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
     * List info about all remote nodes.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     *
     * @return output data
     */
    @GET
    public GenericEntity<List<RemoteInfo>> listRemotes() {
        return new GenericEntity<List<RemoteInfo>>(remotesService.listRemotes()) {
        };
    }
}
