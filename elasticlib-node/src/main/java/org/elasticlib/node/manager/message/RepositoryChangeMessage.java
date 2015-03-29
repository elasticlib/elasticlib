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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
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
        this.repositoryGuid = requireNonNull(repositoryGuid);
    }

    /**
     * @return The repository GUID.
     */
    public Guid getRepositoryGuid() {
        return repositoryGuid;
    }

    @Override
    public int hashCode() {
        return hash(repositoryGuid);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RepositoryChangeMessage other = (RepositoryChangeMessage) obj;
        return repositoryGuid.equals(other.repositoryGuid);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(repositoryGuid)
                .toString();
    }
}
