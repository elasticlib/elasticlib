package store.server.service;

import com.google.common.base.Charsets;
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

    public StorageService(StorageManager storageManager) {
        this.storageManager = storageManager;
        repositoryDefs = storageManager.openDatabase(REPOSITORIES);
        replicationDefs = storageManager.openDatabase(REPLICATIONS);
    }

    public void createRepositoryDef(RepositoryDef def) {
        OperationStatus status = repositoryDefs.putNoOverwrite(currentTransaction(),
                                                               key(def.getName()),
                                                               write(def));
        if (status == OperationStatus.KEYEXIST) {
            throw new RepositoryAlreadyExistsException();
        }
    }

    public boolean deleteRepositoryDef(String name) {
        return repositoryDefs.delete(currentTransaction(), key(name)) == OperationStatus.SUCCESS;
    }

    public RepositoryDef getRepositoryDef(String name) {
        DatabaseEntry entry = new DatabaseEntry();
        OperationStatus status = repositoryDefs.get(currentTransaction(), key(name), entry, LockMode.DEFAULT);
        if (status == OperationStatus.NOTFOUND) {
            throw new UnknownRepositoryException();
        }
        return read(entry, RepositoryDef.class);
    }

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

    public boolean createReplicationDef(ReplicationDef def) {
        return replicationDefs.putNoOverwrite(currentTransaction(),
                                              key(def.getSource(), def.getDestination()),
                                              write(def)) == OperationStatus.SUCCESS;
    }

    public boolean deleteReplicationDef(String source, String destination) {
        return replicationDefs.delete(currentTransaction(), key(source, destination)) == OperationStatus.SUCCESS;
    }

    public void deleteAllReplicationDefs(String name) {
        for (ReplicationDef def : listReplicationDefs()) {
            if (def.getSource().equals(name) || def.getDestination().equals(name)) {
                deleteReplicationDef(def.getSource(), def.getDestination());
            }
        }
    }

    public List<ReplicationDef> listReplicationDefs() {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        List<ReplicationDef> list = new ArrayList<>();
        try (Cursor cursor = storageManager.openCursor(replicationDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                list.add(read(data, ReplicationDef.class));
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
