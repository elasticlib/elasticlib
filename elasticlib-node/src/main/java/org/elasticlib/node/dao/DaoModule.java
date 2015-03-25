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
package org.elasticlib.node.dao;

import org.elasticlib.node.manager.ManagerModule;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides data access objects.
 */
public class DaoModule {

    private final AttributesDao attributesDao;
    private final RepositoriesDao repositoriesDao;
    private final ReplicationsDao replicationsDao;
    private final RemotesDao remotesDao;

    /**
     * Constructor.
     *
     * @param managerModule Manager module.
     */
    public DaoModule(ManagerModule managerModule) {
        StorageManager storageManager = managerModule.getStorageManager();

        attributesDao = new AttributesDao(storageManager);
        repositoriesDao = new RepositoriesDao(storageManager);
        replicationsDao = new ReplicationsDao(storageManager);
        remotesDao = new RemotesDao(storageManager);
    }

    /**
     * @return The attributes DAO.
     */
    public AttributesDao getAttributesDao() {
        return attributesDao;
    }

    /**
     * @return The repositories DAO.
     */
    public RepositoriesDao getRepositoriesDao() {
        return repositoriesDao;
    }

    /**
     * @return The replications DAO.
     */
    public ReplicationsDao getReplicationsDao() {
        return replicationsDao;
    }

    /**
     * @return The remote nodes DAO.
     */
    public RemotesDao getRemotesDao() {
        return remotesDao;
    }
}
