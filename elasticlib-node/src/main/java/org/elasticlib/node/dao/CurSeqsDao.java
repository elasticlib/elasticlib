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
import static org.elasticlib.node.manager.storage.DatabaseEntries.asLong;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Provides a persistent storage for agents sequences.
 * <p>
 * Uses a deferred write database, which does not support transactions.
 */
public class CurSeqsDao {

    private static final String CUR_SEQS = "curSeqs";

    private final Database curSeqs;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     */
    public CurSeqsDao(StorageManager storageManager) {
        this.curSeqs = storageManager.openDeferredWriteDatabase(CUR_SEQS);
    }

    /**
     * Loads sequence associated with key, or 0 if it does not exist.
     *
     * @param key Sequence key.
     * @return Corresponding sequence value or 0.
     */
    public long load(String key) {
        DatabaseEntry value = new DatabaseEntry();
        if (curSeqs.get(null, entry(key), value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            return asLong(value);
        }
        return 0;
    }

    /**
     * Saves sequence associated with supplied key.
     *
     * @param key Sequence key.
     * @param value Sequence value.
     */
    public void save(String key, long value) {
        curSeqs.put(null, entry(key), entry(value));
    }

    /**
     * Deletes sequence associated with supplied key.
     *
     * @param key Sequence key.
     */
    public void delete(String key) {
        curSeqs.delete(null, entry(key));
    }
}
