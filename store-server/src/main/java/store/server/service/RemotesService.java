package store.server.service;

import com.google.common.base.Optional;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.json.JsonObject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import org.glassfish.jersey.client.ClientConfig;
import store.common.NodeDef;
import store.common.hash.Guid;
import static store.common.json.JsonReading.read;
import store.server.exception.UnknownNodeException;
import store.server.exception.UnreachableRemoteException;
import store.server.providers.JsonBodyReader;
import static store.server.storage.DatabaseEntries.asMappable;
import static store.server.storage.DatabaseEntries.entry;
import store.server.storage.Procedure;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Manages remotes nodes.
 */
public class RemotesService {

    private static final String REMOTES = "remotes";
    private final StorageManager storageManager;
    private final Database nodeDefs;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     */
    public RemotesService(StorageManager storageManager) {
        this.storageManager = storageManager;
        nodeDefs = storageManager.openDatabase(REMOTES);
    }

    /**
     * Add a remote node to followed ones. Fails if remote node is not reachable.
     *
     * @param addresses Publish addresses of the remote node.
     */
    public void addRemote(List<String> addresses) {
        for (String address : addresses) {
            Optional<NodeDef> def = downloadDef(address);
            if (def.isPresent()) {
                save(def.get());
                return;
            }
        }
        throw new UnreachableRemoteException();
    }

    private static Optional<NodeDef> downloadDef(String address) {
        ClientConfig clientConfig = new ClientConfig(JsonBodyReader.class);
        Client client = ClientBuilder.newClient(clientConfig);
        try {
            JsonObject json = client.target(address)
                    .request()
                    .get()
                    .readEntity(JsonObject.class);

            return Optional.of(read(json, NodeDef.class));

        } catch (ProcessingException e) {
            return Optional.absent();

        } finally {
            client.close();
        }
    }

    private void save(final NodeDef def) {
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                nodeDefs.put(storageManager.currentTransaction(), entry(def.getGuid()), entry(def));
            }
        });
    }

    /**
     * Delete a remote node definition from persisted ones.
     *
     * @param key Node name or encoded GUID.
     */
    public void removeRemote(final String key) {
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                removeNodeDef(key);
            }
        });
    }

    private void removeNodeDef(final String key) {
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
     * Loads all remote node definitions.
     *
     * @return A list of NodeDef instances.
     */
    public List<NodeDef> listRemotes() {
        List<NodeDef> list = storageManager.inTransaction(new Query<List<NodeDef>>() {
            @Override
            public List<NodeDef> apply() {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry data = new DatabaseEntry();
                List<NodeDef> list = new ArrayList<>();
                try (Cursor cursor = storageManager.openCursor(nodeDefs)) {
                    while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                        list.add(asMappable(data, NodeDef.class));
                    }
                }
                return list;
            }
        });
        Collections.sort(list, new Comparator<NodeDef>() {
            @Override
            public int compare(NodeDef def1, NodeDef def2) {
                return def1.getName().compareTo(def2.getName());
            }
        });
        return list;
    }
}
