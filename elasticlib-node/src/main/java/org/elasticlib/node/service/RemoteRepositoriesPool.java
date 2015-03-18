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
package org.elasticlib.node.service;

import java.util.Optional;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.node.repository.Repository;

/**
 * Manages remote repositories.
 */
public class RemoteRepositoriesPool {

    /**
     * Resolves GUID of a remote repository. Fails it if it is unknown.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding Repository GUID.
     */
    public Guid getRepositoryGuid(String key) {
        // TODO this is a stub !
        throw new UnknownRepositoryException();
    }

    /**
     * Provides definition of a remote repository, if it is known.
     *
     * @param guid Repository GUID.
     * @return Corresponding RepositoryDef, if any.
     */
    public Optional<RepositoryDef> tryGetRepositoryDef(Guid guid) {
        // TODO this is a stub !
        return Optional.empty();
    }

    /**
     * Provides a remote repository. Fails if it unknown or unreachable.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository.
     */
    public Repository getRepository(Guid guid) {
        // TODO this is a stub !
        throw new UnknownRepositoryException();
    }

    /**
     * Provides a remote repository if it is known and currently reachable.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository, if any.
     */
    public Optional<Repository> tryGetRepository(Guid guid) {
        // TODO this is a stub !
        return Optional.empty();
    }
}
