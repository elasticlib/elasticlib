package store.server.service;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import store.common.ReplicationDef;
import store.common.RepositoryDef;
import store.common.hash.Guid;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.UnknownRepositoryException;
import static store.server.storage.DatabaseEntries.asMappable;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;
import static store.server.storage.StorageManager.currentTransaction;

/**
 * Provides a persistant storage for repositories and replications definitions.
 */
class StorageService {

    private static final String REPOSITORIES = "repositories";
    private static final String REPLICATIONS = "replications";
    private final StorageManager storageManager;
    private final Database repositoryDefs;
    private final Database replicationDefs;

    /**
     * Constructor.
     *
     * @param storageManager Underlying storageManager.
     */
    public StorageService(StorageManager storageManager) {
        this.storageManager = storageManager;
        repositoryDefs = storageManager.openDatabase(REPOSITORIES);
        replicationDefs = storageManager.openDatabase(REPLICATIONS);
    }

    /**
     * Creates a new RepositoryDef. Fails if it already exists.
     *
     * @param def RepositoryDef to create.
     */
    public void createRepositoryDef(RepositoryDef def) {
        OperationStatus status = repositoryDefs.putNoOverwrite(currentTransaction(),
                                                               entry(def.getGuid()),
                                                               entry(def));
        if (status == OperationStatus.KEYEXIST) {
            throw new RepositoryAlreadyExistsException();
        }
    }

    /**
     * Optionally deletes a RepositoryDef.
     *
     * @param guid Repository GUID.
     * @return If corresponding RepositoryDef has been found and deleted.
     */
    public boolean deleteRepositoryDef(Guid guid) {
        return repositoryDefs.delete(currentTransaction(), entry(guid)) == OperationStatus.SUCCESS;
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
            OperationStatus status = repositoryDefs.get(currentTransaction(),
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
            Collections.sort(list, new Comparator<RepositoryDef>() {
                @Override
                public int compare(RepositoryDef def1, RepositoryDef def2) {
                    return def1.getName().compareTo(def2.getName());
                }
            });
            return list;
        }
    }

    /**
     * Optionally creates a ReplicationDef, if it does not already exist.
     *
     * @param def A ReplicationDef.
     * @return If it has actually been created.
     */
    public boolean createReplicationDef(ReplicationDef def) {
        return replicationDefs.putNoOverwrite(currentTransaction(),
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
        return replicationDefs.delete(currentTransaction(), entry(source, destination)) == OperationStatus.SUCCESS;
    }

    /**
     * Deletes all replication definitions from/to repository whose GUID is supplied.
     *
     * @param guid Repository GUID.
     */
    public void deleteAllReplicationDefs(Guid guid) {
        for (ReplicationDef def : listReplicationDefs(guid)) {
            deleteReplicationDef(def.getSource(), def.getDestination());
        }
    }

    /**
     * Loads all ReplicationDef.
     *
     * @return All stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs() {
        return listReplicationDefs(Predicates.<ReplicationDef>alwaysTrue());
    }

    /**
     * Loads all ReplicationDef from/to repository whose GUID is supplied.
     *
     * @param guid Repository GUID.
     * @return All matching stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs(final Guid guid) {
        return listReplicationDefs(new Predicate<ReplicationDef>() {
            @Override
            public boolean apply(ReplicationDef def) {
                return def.getSource().equals(guid) || def.getDestination().equals(guid);
            }
        });
    }

    private List<ReplicationDef> listReplicationDefs(Predicate<ReplicationDef> predicate) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        List<ReplicationDef> list = new ArrayList<>();
        try (Cursor cursor = storageManager.openCursor(replicationDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                ReplicationDef def = asMappable(data, ReplicationDef.class);
                if (predicate.apply(def)) {
                    list.add(def);
                }
            }
            return list;
        }
    }
}
