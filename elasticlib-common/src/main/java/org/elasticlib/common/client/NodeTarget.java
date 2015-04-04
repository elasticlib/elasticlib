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

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import static org.elasticlib.common.client.ClientUtil.read;
import org.elasticlib.common.model.NodeInfo;

/**
 * Local node API.
 */
public class NodeTarget {

    private static final String NODE = "node";

    private final WebTarget target;

    /**
     * Constructor.
     *
     * @param target Underlying resource target.
     */
    NodeTarget(WebTarget target) {
        this.target = target.path(NODE);
    }

    /**
     * Provides the info about the node this client is currently connected to.
     *
     * @return A node definition.
     */
    public NodeInfo getInfo() {
        Response response = target.request().get();
        return read(response, NodeInfo.class);
    }
}
