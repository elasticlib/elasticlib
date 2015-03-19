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

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.List;
import java.util.Optional;
import org.elasticlib.common.exception.RepositoryAlreadyExistsException;
import org.elasticlib.common.exception.UnknownRepositoryException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RepositoryDef;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.DatabaseStream;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides a persistent storage for repositories definitions.
 */
public class RepositoriesDao {

    private static final String REPOSITORIES = "repositories";

    private final StorageManager storageManager;
    private final Database repositoryDefs;

    /**
     * Constructor.
     *
     * @param storageManager Underlying storageManager.
     */
    public RepositoriesDao(StorageManager storageManager) {
        this.storageManager = storageManager;
        repositoryDefs = storageManager.openDatabase(REPOSITORIES);
    }

    /**
     * Creates a new RepositoryDef. Fails if it already exists.
     *
     * @param def RepositoryDef to create.
     */
    public void createRepositoryDef(RepositoryDef def) {
        OperationStatus status = repositoryDefs.putNoOverwrite(storageManager.currentTransaction(),
                                                               entry(def.getGuid()),
                                                               entry(def));
        if (status == OperationStatus.KEYEXIST) {
            throw new RepositoryAlreadyExistsException();
        }
    }

    /**
     * Updates a RepositoryDef. Actually creates it if it does not exist.
     *
     * @param def RepositoryDef to update.
     */
    public void updateRepositoryDef(RepositoryDef def) {
        repositoryDefs.put(storageManager.currentTransaction(), entry(def.getGuid()), entry(def));
    }

    /**
     * Optionally deletes a RepositoryDef.
     *
     * @param guid Repository GUID.
     * @return If corresponding RepositoryDef has been found and deleted.
     */
    public boolean deleteRepositoryDef(Guid guid) {
        return repositoryDefs.delete(storageManager.currentTransaction(), entry(guid)) == OperationStatus.SUCCESS;
    }

    /**
     * Loads a RepositoryDef. If key represents a valid encoded GUID, first try to resolve def by GUID. Otherwise
     * returns the first def matching by name with key. Fails if no matching def is found after this second step.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding RepositoryDef.
     */
    public RepositoryDef getRepositoryDef(String key) {
        return tryGetRepositoryDef(key).orElseThrow(UnknownRepositoryException::new);
    }

    /**
     * Loads a RepositoryDef, if it exists. If key represents a valid encoded GUID, first try to resolve def by GUID.
     * Otherwise returns the first def matching by name with key.
     *
     * @param key Repository name or encoded GUID.
     * @return Corresponding RepositoryDef, if any.
     */
    public Optional<RepositoryDef> tryGetRepositoryDef(String key) {
        if (Guid.isValid(key)) {
            Optional<RepositoryDef> def = tryGetRepositoryDef(new Guid(key));
            if (def.isPresent()) {
                return def;
            }
        }
        return stream().first(def -> def.getName().equals(key));
    }

    /**
     * Loads a RepositoryDef, if it exists.
     *
     * @param guid Repository GUID.
     * @return Corresponding RepositoryDef, if any.
     */
    public Optional<RepositoryDef> tryGetRepositoryDef(Guid guid) {
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = repositoryDefs.get(storageManager.currentTransaction(),
                                                    entry(guid),
                                                    entry,
                                                    LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) {
            return Optional.empty();
        }
        return Optional.of(asMappable(entry, RepositoryDef.class));
    }

    /**
     * Loads all RepositoryDef.
     *
     * @return All stored repository definitions.
     */
    public List<RepositoryDef> listRepositoryDefs() {
        return stream().orderBy(RepositoryDef::getName).list();
    }

    private DatabaseStream<RepositoryDef> stream() {
        return storageManager.stream(repositoryDefs, RepositoryDef.class);
    }
}
