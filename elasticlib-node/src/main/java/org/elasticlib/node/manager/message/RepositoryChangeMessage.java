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
package org.elasticlib.node.manager.message;

import org.elasticlib.common.hash.Guid;

/**
 * Base class for messages related to a repository state change.
 */
public abstract class RepositoryChangeMessage {

    private final Guid repositoryGuid;

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public RepositoryChangeMessage(Guid repositoryGuid) {
        this.repositoryGuid = repositoryGuid;
    }

    /**
     * @return The repository GUID.
     */
    public Guid getRepositoryGuid() {
        return repositoryGuid;
    }
}
