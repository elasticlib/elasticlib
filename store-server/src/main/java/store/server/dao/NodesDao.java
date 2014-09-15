package store.server.dao;

import com.google.common.base.Predicate;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import static java.util.Collections.sort;
import java.util.Comparator;
import java.util.List;
import store.common.NodeInfo;
import store.common.hash.Guid;
import store.server.exception.NodeAlreadyTrackedException;
import store.server.exception.UnknownNodeException;
import static store.server.storage.DatabaseEntries.asMappable;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.StorageManager;

/**
 * Provides a persistent storage for nodes infos.
 */
public class NodesDao {

    private static final String NODES = "nodes";
    private final StorageManager storageManager;
    private final Database nodeInfos;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     */
    public NodesDao(StorageManager storageManager) {
        this.storageManager = storageManager;
        nodeInfos = storageManager.openDatabase(NODES);
    }

    /**
     * Checks whether a NodeInfo with supplied GUID is already stored.
     *
     * @param guid A node GUID.
     * @return true if associated node definition is already stored.
     */
    public boolean containsNodeInfo(Guid guid) {
        OperationStatus status = nodeInfos.get(storageManager.currentTransaction(),
                                               entry(guid),
                                               new DatabaseEntry(),
                                               LockMode.DEFAULT);

        return status == OperationStatus.SUCCESS;
    }

    /**
     * Creates a new NodeInfo if it does not exist, does nothing otherwise.
     *
     * @param info NodeInfo to save.
     */
    public void saveNodeInfo(NodeInfo info) {
        nodeInfos.put(storageManager.currentTransaction(), entry(info.getDef().getGuid()), entry(info));
    }

    /**
     * Creates a new NodeInfo. Fails if it already exist.
     *
     * @param info NodeInfo to save.
     */
    public void createNodeInfo(NodeInfo info) {
        OperationStatus status = nodeInfos.put(storageManager.currentTransaction(),
                                               entry(info.getDef().getGuid()),
                                               entry(info));

        if (status == OperationStatus.KEYEXIST) {
            throw new NodeAlreadyTrackedException();
        }
    }

    /**
     * Deletes a NodeInfo. Fail if it does not exist.
     *
     * @param key Node name or encoded GUID.
     */
    public void deleteNodeInfo(String key) {
        if (Guid.isValid(key)) {
            OperationStatus status = nodeInfos.delete(storageManager.currentTransaction(), entry(new Guid(key)));
            if (status == OperationStatus.SUCCESS) {
                return;
            }
        }
        DatabaseEntry curKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(nodeInfos)) {
            while (cursor.getNext(curKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                NodeInfo info = asMappable(data, NodeInfo.class);
                if (info.getDef().getName().equals(key)) {
                    cursor.delete();
                    return;
                }
            }
        }
        throw new UnknownNodeException();
    }

    /**
     * Loads all NodeInfo matching supplied predicate.
     *
     * @param predicate Filtering predicate.
     * @return Matching stored node infos.
     */
    public List<NodeInfo> listNodeInfos(Predicate<NodeInfo> predicate) {
        List<NodeInfo> list = new ArrayList<>();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(nodeInfos)) {
            while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                NodeInfo info = asMappable(data, NodeInfo.class);
                if (predicate.apply(info)) {
                    list.add(asMappable(data, NodeInfo.class));
                }
            }
        }
        sort(list, new Comparator<NodeInfo>() {
            @Override
            public int compare(NodeInfo info1, NodeInfo info2) {
                return info1.getDef().getName().compareTo(info2.getDef().getName());
            }
        });
        return list;
    }
}
