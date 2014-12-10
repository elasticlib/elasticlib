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
     * Optionally creates a ReplicationDef, if it does not already exist.
     *
     * @param def A ReplicationDef.
     * @return If it has actually been created.
     */
    public boolean createReplicationDef(ReplicationDef def) {
        return replicationDefs.putNoOverwrite(storageManager.currentTransaction(),
                                              entry(def.getSource(), def.getDestination()),
                                              entry(def)) == OperationStatus.SUCCESS;
    }

    /**
     * Optionally deletes a ReplicationDef.
     *
     * @param source Source repository GUID.
     * @param destination Destination repository GUID.
     * @return If corresponding ReplicationDef has been found and deleted.
     */
    public boolean deleteReplicationDef(Guid source, Guid destination) {
        OperationStatus status = replicationDefs.delete(storageManager.currentTransaction(),
                                                        entry(source, destination));
        return status == OperationStatus.SUCCESS;
    }

    /**
     * Deletes all replication definitions from/to repository whose GUID is supplied.
     *
     * @param guid Repository GUID.
     */
    public void deleteAllReplicationDefs(Guid guid) {
        listReplicationDefs(guid)
                .stream()
                .forEach(def -> deleteReplicationDef(def.getSource(), def.getDestination()));
    }

    /**
     * Loads a ReplicationDef. Fails if no replication is associated with supplied repository GUIDs.
     *
     * @param source Source repository GUID.
     * @param destination Destination repository GUID.
     * @return Corresponding ReplicationDef.
     */
    public ReplicationDef getReplicationDef(Guid source, Guid destination) {
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = replicationDefs.get(storageManager.currentTransaction(),
                                                     entry(source, destination),
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
        return listReplicationDefs(x -> true);
    }

    /**
     * Loads all ReplicationDef from/to repository whose GUID is supplied.
     *
     * @param guid Repository GUID.
     * @return All matching stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs(Guid guid) {
        return listReplicationDefs(def -> def.getSource().equals(guid) || def.getDestination().equals(guid));
    }

    private List<ReplicationDef> listReplicationDefs(Predicate<ReplicationDef> predicate) {
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
