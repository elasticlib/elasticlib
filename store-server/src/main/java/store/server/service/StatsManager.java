package store.server.service;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import store.common.RepositoryStats;
import static store.server.storage.DatabaseEntries.asMappable;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.Procedure;
import store.server.storage.StorageManager;
import static store.server.storage.StorageManager.currentTransaction;

/**
 * Manages repository stats
 */
class StatsManager {

    private static final String STATS = "stats";
    private final StorageManager storageManager;
    private final Database statsDb;
    private final AtomicReference<RepositoryStats> latestSnapshot;

    /**
     * Constructor.
     *
     * @param storageManager Repository storage manager.
     */
    public StatsManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        statsDb = storageManager.openDatabase(STATS);
        latestSnapshot = new AtomicReference<>();
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                latestSnapshot.set(loadPersistedStats());
            }
        });
    }

    private RepositoryStats loadPersistedStats() {
        DatabaseEntry key = entry(STATS);
        DatabaseEntry data = new DatabaseEntry();
        if (statsDb.get(currentTransaction(), key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
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
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                statsDb.put(currentTransaction(), entry(stats), entry(stats));
            }
        });
        latestSnapshot.set(stats);
    }
}
