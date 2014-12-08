package org.elasticlib.node.dao;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.elasticlib.common.exception.NodeAlreadyTrackedException;
import org.elasticlib.common.exception.UnknownNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeInfo;
import static org.elasticlib.node.manager.storage.DatabaseEntries.asMappable;
import static org.elasticlib.node.manager.storage.DatabaseEntries.entry;
import org.elasticlib.node.manager.storage.StorageManager;

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
     * Deletes info of all unreachable nodes.
     */
    public void deleteUnreachableNodeInfos() {
        DatabaseEntry curKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (Cursor cursor = storageManager.openCursor(nodeInfos)) {
            while (cursor.getNext(curKey, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                NodeInfo info = asMappable(data, NodeInfo.class);
                if (!info.isReachable()) {
                    cursor.delete();
                }
            }
        }
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
                if (predicate.test(info)) {
                    list.add(asMappable(data, NodeInfo.class));
                }
            }
        }
        list.sort((a, b) -> a.getDef().getName().compareTo(b.getDef().getName()));
        return list;
    }
}
