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
import java.util.List;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.elasticlib.common.exception.BadRequestException;
import org.elasticlib.common.hash.Guid;
import static org.elasticlib.common.json.JsonValidation.hasStringValue;
import org.elasticlib.common.model.ReplicationInfo;
import org.elasticlib.node.service.ReplicationsService;

/**
 * Replications REST resource.
 */
@Path("replications")
public class ReplicationsResource {

    private static final String ACTION = "action";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String REPLICATION = "replication";

    @Inject
    private ReplicationsService replicationsService;
    @Context
    private UriInfo uriInfo;

    /**
     * Alters state of a replication.
     * <p>
     * Input:<br>
     * - source (String): Source name or encoded GUID.<br>
     * - target (String): Target name or encoded GUID.
     * <p>
     * Response:<br>
     * - 200 OK: Operation succeeded.<br>
     * - 201 CREATED: Creation succeeded.<br>
     * - 400 BAD REQUEST: Invalid JSON data.<br>
     * - 404 NOT FOUND: Source or target repository, or replication was not found.<br>
     * - 412 PRECONDITION FAILED: Replication to create already exists.
     *
     * @param json input JSON data
     * @return HTTP response
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postReplication(JsonObject json) {
        Action action = Action.of(json);
        switch (action) {
            case CREATE:
                return createReplication(json);
            case START:
                return startReplication(json);
            case STOP:
                return stopReplication(json);
            case DELETE:
                return deleteReplication(json);
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

    private Response createReplication(JsonObject json) {
        String source = string(json, SOURCE);
        String target = string(json, TARGET);

        replicationsService.createReplication(source, target);
        URI location = uriInfo.getAbsolutePathBuilder()
                .queryParam(SOURCE, source)
                .queryParam(TARGET, target)
                .build();

        return Response.created(location).build();

    }

    private Response startReplication(JsonObject json) {
        replicationsService.startReplication(guid(json, REPLICATION));
        return Response.ok().build();
    }

    private Response stopReplication(JsonObject json) {
        replicationsService.stopReplication(guid(json, REPLICATION));
        return Response.ok().build();
    }

    private Response deleteReplication(JsonObject json) {
        return deleteReplication(guid(json, REPLICATION));
    }

    private static String string(JsonObject json, String key) {
        if (!hasStringValue(json, key)) {
            throw newInvalidJsonException();
        }
        return json.getString(key);
    }

    private static Guid guid(JsonObject json, String key) {
        String hexValue = string(json, key);
        if (!Guid.isValid(hexValue)) {
            throw newInvalidJsonException();
        }
        return new Guid(hexValue);
    }

    /**
     * Deletes a replication.
     * <p>
     * Response:<br>
     * - 20O OK: Operation succeeded.<br>
     * - 404 NOT FOUND: Replication was not found.<br>
     *
     * @param guid Replication GUID
     * @return HTTP response
     */
    @DELETE
    @Path("{replication}")
    public Response deleteReplication(@PathParam(REPLICATION) Guid guid) {
        replicationsService.deleteReplication(guid);
        return Response.ok().build();
    }

    /**
     * Lists info about existing replications.
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
