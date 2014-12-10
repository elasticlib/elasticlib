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
import org.elasticlib.common.hash.Guid;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asGuid;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides a persistent storage for various attributes.
 */
public class AttributesDao {

    private static final String ATTRIBUTES = "attributes";
    private static final String GUID = "guid";

    private final StorageManager storageManager;
    private final Database attributes;

    /**
     * Constructor.
     *
     * @param storageManager Underlying storageManager.
     */
    public AttributesDao(StorageManager storageManager) {
        this.storageManager = storageManager;
        attributes = storageManager.openDatabase(ATTRIBUTES);
    }

    /**
     * Provides the GUID of the local node. If it does not exists, it is randomly generated.
     *
     * @return A GUID instance.
     */
    public Guid guid() {
        DatabaseEntry key = entry(GUID);
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus status = attributes.get(storageManager.currentTransaction(), key, value, LockMode.RMW);
        if (status == OperationStatus.SUCCESS) {
            return asGuid(value);
        }
        Guid newGuid = Guid.random();
        attributes.put(storageManager.currentTransaction(), key, entry(newGuid));
        return newGuid;
    }
}
