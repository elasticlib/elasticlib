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
package org.elasticlib.node.repository;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.elasticlib.common.model.RepositoryStats;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

/**
 * Manages repository stats.
 */
class StatsManager {

    private static final String STATS = "stats";

    private final StorageManager storageManager;
    private final Database statsDb;
    private final DatabaseEntry statsKey;
    private final AtomicReference<RepositoryStats> latestSnapshot;

    /**
     * Constructor.
     *
     * @param storageManager Repository storage manager.
     */
    public StatsManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        statsDb = storageManager.openDatabase(STATS);
        statsKey = entry(STATS);
        latestSnapshot = new AtomicReference<>();
        storageManager.inTransaction(() -> latestSnapshot.set(loadPersistedStats()));
    }

    private RepositoryStats loadPersistedStats() {
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = statsDb.get(storageManager.currentTransaction(), statsKey, data, LockMode.DEFAULT);
        if (status == OperationStatus.SUCCESS) {
            return asMappable(data, RepositoryStats.class);
        }
        return new RepositoryStats(0, 0, 0, Collections.<String, Long>emptyMap());
    }

    /**
     * Provides a consistent snapshot of statistics about this repository. As statistics are computed asynchronously,
     * returned snapshot might be not completely up to date.
     *
     * @return A RepositoryStats instance.
     */
    public RepositoryStats stats() {
        return latestSnapshot.get();
    }

    /**
     * Replace current statistics snapshot by supplied one.
     *
     * @param stats A RepositoryStats instance.
     */
    public void update(final RepositoryStats stats) {
        storageManager.inTransaction(() -> statsDb.put(storageManager.currentTransaction(), statsKey, entry(stats)));
        latestSnapshot.set(stats);
    }
}
