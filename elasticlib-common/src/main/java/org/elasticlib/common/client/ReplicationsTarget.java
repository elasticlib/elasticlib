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

import static com.google.common.collect.ImmutableMap.of;
import java.net.URI;
import java.util.List;
import java.util.Map;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonObjectBuilder;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static org.elasticlib.common.client.ClientUtil.ensureSuccess;
import static org.elasticlib.common.client.ClientUtil.readAll;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.ReplicationInfo;

/**
 * Replications API.
 */
public class ReplicationsTarget {

    private static final String REPLICATIONS = "replications";
    private static final String REPLICATION = "replication";
    private static final String REPLICATION_TEMPLATE = "{replication}";
    private static final String ACTION = "action";
    private static final String CREATE = "create";
    private static final String START = "start";
    private static final String STOP = "stop";
    private static final String SOURCE = "source";
    private static final String TARGET = "target";

    private final WebTarget target;

    /**
     * Constructor.
     *
     * @param target Underlying resource target.
     */
    ReplicationsTarget(WebTarget target) {
        this.target = target.path(REPLICATIONS);
    }

    /**
     * @return The URI identifying this resource target.
     */
    public URI getUri() {
        return target.getUri();
    }

    /**
     * Creates a new replication.
     *
     * @param source Source repository.
     * @param target Target repository.
     */
    public void create(String source, String target) {
        post(of(ACTION, CREATE,
                SOURCE, source,
                TARGET, target));
    }

    /**
     * Starts an existing replication.
     *
     * @param replication Replication GUID.
     */
    public void start(Guid replication) {
        post(of(ACTION, START,
                REPLICATION, replication.asHexadecimalString()));
    }

    /**
     * Stops an existing replication.
     *
     * @param replication Replication GUID.
     */
    public void stop(Guid replication) {
        post(of(ACTION, STOP,
                REPLICATION, replication.asHexadecimalString()));
    }

    private void post(Map<String, String> values) {
        JsonObjectBuilder builder = createObjectBuilder();
        values.forEach(builder::add);

        ensureSuccess(target
                .request()
                .post(json(builder.build())));
    }

    /**
     * Deletes an existing replication.
     *
     * @param replication Replication GUID.
     */
    public void delete(Guid replication) {
        ensureSuccess(target
                .path(REPLICATION_TEMPLATE)
                .resolveTemplate(REPLICATION, replication.asHexadecimalString())
                .request()
                .delete());
    }

    /**
     * Lists infos of existing replications.
     *
     * @return A list of replication infos.
     */
    public List<ReplicationInfo> listInfos() {
        Response response = target
                .request()
                .get();

        return readAll(response, ReplicationInfo.class);
    }
}
