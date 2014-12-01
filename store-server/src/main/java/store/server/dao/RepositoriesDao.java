package store.server.dao;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.List;
import store.common.exception.RepositoryAlreadyExistsException;
import store.common.exception.UnknownRepositoryException;
import store.common.hash.Guid;
import store.common.model.RepositoryDef;
import static store.server.manager.storage.DatabaseEntries.asMappable;
import static store.server.manager.storage.DatabaseEntries.entry;
import store.server.manager.storage.StorageManager;

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
        if (Guid.isValid(key)) {
            DatabaseEntry entry = new DatabaseEntry();
            OperationStatus status = repositoryDefs.get(storageManager.currentTransaction(),
                                                        entry(new Guid(key)),
                                                        entry,
                                                        LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS) {
                return asMappable(entry, RepositoryDef.class);
            }
        }
        for (RepositoryDef def : listRepositoryDefs()) {
            if (def.getName().equals(key)) {
                return def;
            }
        }
        throw new UnknownRepositoryException();
    }

    /**
     * Loads all RepositoryDef.
     *
     * @return All stored repository definitions.
     */
    public List<RepositoryDef> listRepositoryDefs() {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        List<RepositoryDef> list = new ArrayList<>();
        try (Cursor cursor = storageManager.openCursor(repositoryDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                list.add(asMappable(data, RepositoryDef.class));
            }
        }
        list.sort((a, b) -> a.getName().compareTo(b.getName()));
        return list;
    }
}
