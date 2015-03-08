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

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.elasticlib.common.exception.ReplicationAlreadyExistsException;
import org.elasticlib.common.exception.UnknownReplicationException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.ReplicationDef;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides a persistent storage for replications definitions.
 */
public class ReplicationsDao {

    private static final String REPLICATIONS = "replications";

    private final StorageManager storageManager;
    private final Database replicationDefs;

    /**
     * Constructor.
     *
     * @param storageManager Underlying storageManager.
     */
    public ReplicationsDao(StorageManager storageManager) {
        this.storageManager = storageManager;
        replicationDefs = storageManager.openDatabase(REPLICATIONS);
    }

    /**
     * Creates a new ReplicationDef. Fails if it does already exist.
     *
     * @param def A ReplicationDef.
     */
    public void createReplicationDef(ReplicationDef def) {
        if (any(x -> x.getSource().equals(def.getSource()) &&
                x.getDestination().equals(def.getDestination()))) {
            throw new ReplicationAlreadyExistsException();
        }
        replicationDefs.put(storageManager.currentTransaction(), entry(def.getGuid()), entry(def));
    }

    /**
     * Deletes a ReplicationDef. Fails if it does not exists
     *
     * @param guid Replication GUID.
     */
    public void deleteReplicationDef(Guid guid) {
        OperationStatus status = replicationDefs.delete(storageManager.currentTransaction(), entry(guid));
        if (status != OperationStatus.SUCCESS) {
            throw new UnknownReplicationException();
        }
    }

    /**
     * Loads a ReplicationDef. Fails if no replication is associated with supplied GUID.
     *
     * @param guid Replication GUID.
     * @return Corresponding ReplicationDef.
     */
    public ReplicationDef getReplicationDef(Guid guid) {
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = replicationDefs.get(storageManager.currentTransaction(),
                                                     entry(guid),
                                                     entry,
                                                     LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) {
            throw new UnknownReplicationException();
        }
        return asMappable(entry, ReplicationDef.class);
    }

    /**
     * Loads all ReplicationDef.
     *
     * @return All stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs() {
        return list(x -> true);
    }

    /**
     * Loads all ReplicationDef from repository whose GUID is supplied.
     *
     * @param guid Source repository GUID.
     * @return All matching stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefsFrom(Guid guid) {
        return list(def -> def.getSource().equals(guid));
    }

    /**
     * Loads all ReplicationDef from/to repository whose GUID is supplied.
     *
     * @param guid Repository GUID.
     * @return All matching stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs(Guid guid) {
        return list(def -> def.getSource().equals(guid) || def.getDestination().equals(guid));
    }

    private boolean any(Predicate<ReplicationDef> predicate) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(replicationDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                ReplicationDef def = asMappable(data, ReplicationDef.class);
                if (predicate.test(def)) {
                    return true;
                }
            }
            return false;
        }
    }

    private List<ReplicationDef> list(Predicate<ReplicationDef> predicate) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        List<ReplicationDef> list = new ArrayList<>();
        try (Cursor cursor = storageManager.openCursor(replicationDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                ReplicationDef def = asMappable(data, ReplicationDef.class);
                if (predicate.test(def)) {
                    list.add(def);
                }
            }
            return list;
        }
    }
}
