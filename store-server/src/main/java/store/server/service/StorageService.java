package store.server.service;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.List;
import store.common.Mappable;
import store.common.MappableUtil;
import store.common.ReplicationDef;
import store.common.RepositoryDef;
import store.common.bson.BsonReader;
import store.common.bson.BsonWriter;
import store.server.exception.RepositoryAlreadyExistsException;
import store.server.exception.UnknownRepositoryException;
import store.server.storage.StorageManager;
import static store.server.storage.StorageManager.currentTransaction;

/**
 * Provides a persistant storage for repositories and replications definitions.
 */
class StorageService {

    private static final String REPOSITORIES = "repositories";
    private static final String REPLICATIONS = "replications";
    private static final String SOURCE = "source";
    private static final String DESTINATION = "destination";
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
                                                               key(def.getName()),
                                                               write(def));
        if (status == OperationStatus.KEYEXIST) {
            throw new RepositoryAlreadyExistsException();
        }
    }

    /**
     * Optionally deletes a RepositoryDef.
     *
     * @param name Repository name.
     * @return If corresponding RepositoryDef has been found and deleted.
     */
    public boolean deleteRepositoryDef(String name) {
        return repositoryDefs.delete(currentTransaction(), key(name)) == OperationStatus.SUCCESS;
    }

    /**
     * Loads a RepositoryDef.
     *
     * @param name Repository name.
     * @return Corresponding RepositoryDef.
     */
    public RepositoryDef getRepositoryDef(String name) {
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = repositoryDefs.get(currentTransaction(), key(name), entry, LockMode.DEFAULT);
        if (status == OperationStatus.NOTFOUND) {
            throw new UnknownRepositoryException();
        }
        return read(entry, RepositoryDef.class);
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
                list.add(read(data, RepositoryDef.class));
            }
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
                                              key(def.getSource(), def.getDestination()),
                                              write(def)) == OperationStatus.SUCCESS;
    }

    /**
     * Optionally deletes a ReplicationDef.
     *
     * @param source Source repository name.
     * @param destination Destination repository name.
     * @return If corresponding ReplicationDef has been found and deleted.
     */
    public boolean deleteReplicationDef(String source, String destination) {
        return replicationDefs.delete(currentTransaction(), key(source, destination)) == OperationStatus.SUCCESS;
    }

    /**
     * Deletes all replication definitions from/to repository whose name is supplied.
     *
     * @param name Repository name.
     */
    public void deleteAllReplicationDefs(String name) {
        for (ReplicationDef def : listReplicationDefs(name)) {
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
     * Loads all ReplicationDef from/to repository whose name is supplied.
     *
     * @param name Repository name.
     * @return All matching stored replication definitions.
     */
    public List<ReplicationDef> listReplicationDefs(final String name) {
        return listReplicationDefs(new Predicate<ReplicationDef>() {
            @Override
            public boolean apply(ReplicationDef def) {
                return def.getSource().equals(name) || def.getDestination().equals(name);
            }
        });
    }

    private List<ReplicationDef> listReplicationDefs(Predicate<ReplicationDef> predicate) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        List<ReplicationDef> list = new ArrayList<>();
        try (Cursor cursor = storageManager.openCursor(replicationDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                ReplicationDef def = read(data, ReplicationDef.class);
                if (predicate.apply(def)) {
                    list.add(def);
                }
            }
            return list;
        }
    }

    private static DatabaseEntry key(String name) {
        return new DatabaseEntry(name.getBytes(Charsets.UTF_8));
    }

    private static DatabaseEntry key(String source, String destination) {
        return new DatabaseEntry(new BsonWriter()
                .put(SOURCE, source)
                .put(DESTINATION, destination)
                .build());
    }

    private static DatabaseEntry write(Mappable mappable) {
        return new DatabaseEntry(new BsonWriter().put(mappable.toMap()).build());
    }

    private static <T extends Mappable> T read(DatabaseEntry entry, Class<T> clazz) {
        return MappableUtil.fromMap(new BsonReader(entry.getData()).asMap(), clazz);
    }
}
