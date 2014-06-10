package store.server.service;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import store.common.Operation;
import static store.common.Operation.CREATE;
import static store.common.Operation.DELETE;
import static store.common.Operation.UPDATE;
import store.common.RepositoryStats;
import static store.server.storage.DatabaseEntries.asLong;
import static store.server.storage.DatabaseEntries.asString;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.Procedure;
import store.server.storage.StorageManager;
import static store.server.storage.StorageManager.currentTransaction;

/**
 * Manages repository stats
 */
class StatsManager {

    private static final String OPERATIONS_STATS = "operationsStats";
    private static final String METADATA_STATS = "metadataStats";
    private final StorageManager storageManager;
    private final Database eventStatsDb;
    private final Database metadataStatsDb;
    private AtomicReference<RepositoryStats> latestSnapshot;

    /**
     * Constructor.
     *
     * @param storageManager Repository storage manager.
     */
    public StatsManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        eventStatsDb = storageManager.openDatabase(OPERATIONS_STATS);
        metadataStatsDb = storageManager.openDatabase(METADATA_STATS);

        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                latestSnapshot = new AtomicReference<>(new RepositoryStats(load(CREATE),
                                                                           load(UPDATE),
                                                                           load(DELETE),
                                                                           loadMetadataCounts()));
            }
        });
    }

    private long load(Operation operation) {
        DatabaseEntry key = entry(operation.toString());
        DatabaseEntry data = new DatabaseEntry();
        if (eventStatsDb.get(currentTransaction(), key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            return asLong(data);
        }
        return 0;
    }

    private Map<String, Long> loadMetadataCounts() {
        try (Cursor cursor = storageManager.openCursor(metadataStatsDb)) {
            Map<String, Long> counts = new TreeMap<>();
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                counts.put(asString(key), asLong(data));
            }
            return counts;
        }
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
     * Replace current statistics snapshot of by supplied one.
     *
     * @param stats A RepositoryStats instance.
     */
    public void update(final RepositoryStats stats) {
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                update(CREATE, stats.getCreations());
                update(UPDATE, stats.getUpdates());
                update(DELETE, stats.getDeletions());
                update(stats.getMetadataCounts());
            }
        });
        latestSnapshot.set(stats);
    }

    private void update(Operation operation, long count) {
        eventStatsDb.put(currentTransaction(), entry(operation.toString()), entry(count));
    }

    private void update(Map<String, Long> metadataCounts) {
        try (Cursor cursor = storageManager.openCursor(metadataStatsDb)) {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                Long count = metadataCounts.get(asString(key));
                if (count != null) {
                    cursor.putCurrent(entry(count));
                } else {
                    cursor.delete();
                }
            }
        }
    }
}
