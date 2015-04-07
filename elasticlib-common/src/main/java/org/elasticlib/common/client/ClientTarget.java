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
import javax.ws.rs.client.WebTarget;

/**
 * Base API on a given node.
 */
public class ClientTarget {

    private final WebTarget target;
    private final NodeTarget nodeTarget;
    private final RemotesTarget remotesTarget;
    private final RepositoriesTarget repositoriesTarget;
    private final ReplicationsTarget replicationsTarget;

    /**
     * Constructor.
     *
     * @param target Underlying resource target.
     */
    ClientTarget(WebTarget target) {
        this.target = target;
        nodeTarget = new NodeTarget(target);
        remotesTarget = new RemotesTarget(target);
        repositoriesTarget = new RepositoriesTarget(target);
        replicationsTarget = new ReplicationsTarget(target);
    }

    /**
     * @return The URI identifying this resource target.
     */
    public URI getUri() {
        return target.getUri();
    }

    /**
     * @return The local node API.
     */
    public NodeTarget node() {
        return nodeTarget;
    }

    /**
     * @return The remote nodes API.
     */
    public RemotesTarget remotes() {
        return remotesTarget;
    }

    /**
     * @return The repositories API.
     */
    public RepositoriesTarget repositories() {
        return repositoriesTarget;
    }

    /**
     * @return The replications API.
     */
    public ReplicationsTarget replications() {
        return replicationsTarget;
    }
}
