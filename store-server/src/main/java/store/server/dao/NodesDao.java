package store.server.dao;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import static java.util.Collections.sort;
import java.util.Comparator;
import java.util.List;
import store.common.NodeDef;
import store.common.hash.Guid;
import store.server.exception.NodeAlreadyTrackedException;
import store.server.exception.UnknownNodeException;
import static store.server.storage.DatabaseEntries.asMappable;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;

/**
 * Provides a persistent storage for nodes definitions.
 */
public class NodesDao {

    private static final String NODES = "nodes";
    private final StorageManager storageManager;
    private final Database nodeDefs;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     */
    public NodesDao(StorageManager storageManager) {
        this.storageManager = storageManager;
        nodeDefs = storageManager.openDatabase(NODES);
    }

    /**
     * Checks whether a NodeDef with supplied GUID is already stored.
     *
     * @param guid A node GUID.
     * @return true if associated node definition is already stored.
     */
    public boolean containsNodeDef(Guid guid) {
        OperationStatus status = nodeDefs.get(storageManager.currentTransaction(),
                                              entry(guid),
                                              new DatabaseEntry(),
                                              LockMode.DEFAULT);

        return status == OperationStatus.SUCCESS;
    }

    /**
     * Creates a new NodeDef if it does not exist, does nothing otherwise.
     *
     * @param def NodeDef to save.
     */
    public void saveNodeDef(NodeDef def) {
        nodeDefs.put(storageManager.currentTransaction(), entry(def.getGuid()), entry(def));
    }

    /**
     * Creates a new NodeDef. Fails if it already exist.
     *
     * @param def NodeDef to create.
     */
    public void createNodeDef(NodeDef def) {
        OperationStatus status = nodeDefs.put(storageManager.currentTransaction(),
                                              entry(def.getGuid()),
                                              entry(def));

        if (status == OperationStatus.KEYEXIST) {
            throw new NodeAlreadyTrackedException();
        }
    }

    /**
     * Deletes a NodeDef. Fail if it does not exist.
     *
     * @param key Node name or encoded GUID.
     */
    public void deleteNodeDef(String key) {
        if (Guid.isValid(key)) {
            OperationStatus status = nodeDefs.delete(storageManager.currentTransaction(), entry(new Guid(key)));
            if (status == OperationStatus.SUCCESS) {
                return;
            }
        }
        DatabaseEntry curKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(nodeDefs)) {
            while (cursor.getNext(curKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                NodeDef def = asMappable(data, NodeDef.class);
                if (def.getName().equals(key)) {
                    cursor.delete();
                    return;
                }
            }
        }
        throw new UnknownNodeException();
    }

    /**
     * Loads all NodeDef.
     *
     * @return All stored node definitions.
     */
    public List<NodeDef> listNodeDefs() {
        List<NodeDef> list = new ArrayList<>();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(nodeDefs)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                list.add(asMappable(data, NodeDef.class));
            }
        }
        sort(list, new Comparator<NodeDef>() {
            @Override
            public int compare(NodeDef def1, NodeDef def2) {
                return def1.getName().compareTo(def2.getName());
            }
        });
        return list;
    }
}
