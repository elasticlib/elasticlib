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
package org.elasticlib.node.components;

import java.util.Optional;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import org.elasticlib.node.repository.Repository;

/**
 * Provides local and remote repositories.
 */
public class RepositoriesProvider {

    private final LocalRepositoriesPool localRepositoriesPool;
    private final RemoteRepositoriesPool remoteRepositoriesPool;

    /**
     * Constructor.
     *
     * @param localRepositoriesPool Local repositories pool.
     * @param remoteRepositoriesPool Remote repositories pool.
     */
    public RepositoriesProvider(LocalRepositoriesPool localRepositoriesPool,
                                RemoteRepositoriesPool remoteRepositoriesPool) {
        this.localRepositoriesPool = localRepositoriesPool;
        this.remoteRepositoriesPool = remoteRepositoriesPool;
    }

    /**
     * Resolves GUID of a repository. Fails if it is unknown.
     * <p>
     * Resolution rules:<br>
     * - If supplied key matches <i>node.repository</i> a remote repository is searched.<br>
     * - Otherwise first a local repository is searched, and then, if there is no result, a remote one is searched.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding Repository GUID.
     */
    public Guid getRepositoryGuid(String key) {
        if (key.contains(".")) {
            return remoteRepositoriesPool.getRepositoryGuid(key);
        }
        Optional<Guid> local = localRepositoriesPool.tryGetRepositoryGuid(key);
        if (local.isPresent()) {
            return local.get();
        }
        return remoteRepositoriesPool.getRepositoryGuid(key);
    }

    /**
     * Provides definition of a repository. Fails if it is unknown.
     *
     * @param guid Repository GUID.
     * @return Corresponding RepositoryDef.
     */
    public RepositoryDef getRepositoryDef(Guid guid) {
        Optional<RepositoryDef> local = localRepositoriesPool.tryGetRepositoryDef(guid);
        if (local.isPresent()) {
            return local.get();
        }
        return remoteRepositoriesPool.tryGetRepositoryDef(guid)
                .orElseThrow(UnknownRepositoryException::new);
    }

    /**
     * Provides a repository. Fails if it is unknown or unreachable.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository.
     */
    public Repository getRepository(Guid guid) {
        Optional<Repository> local = localRepositoriesPool.tryGetRepositoryIfOpen(guid);
        if (local.isPresent()) {
            return local.get();
        }
        return remoteRepositoriesPool.getRepository(guid);
    }

    /**
     * Provides a repository if it is known and currently reachable.
     *
     * @param guid Repository GUID.
     * @return Corresponding repository, if available.
     */
    public Optional<Repository> tryGetRepository(Guid guid) {
        Optional<Repository> local = localRepositoriesPool.tryGetRepository(guid);
        if (local.isPresent()) {
            return local;
        }
        return remoteRepositoriesPool.tryGetRepository(guid);
    }
}
