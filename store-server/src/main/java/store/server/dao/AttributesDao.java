package store.server.dao;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import store.common.hash.Guid;
import static store.server.manager.storage.DatabaseEntries.asGuid;
import static store.server.manager.storage.DatabaseEntries.entry;
import store.server.manager.storage.StorageManager;

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
