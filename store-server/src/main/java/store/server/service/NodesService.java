package store.server.service;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import java.net.URI;
import java.util.List;
import javax.json.JsonObject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import org.glassfish.jersey.client.ClientConfig;
import static org.joda.time.Instant.now;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.NodeDef;
import store.common.NodeInfo;
import store.common.hash.Guid;
import static store.common.json.JsonReading.read;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.exception.SelfTrackingException;
import store.server.exception.UnreachableNodeException;
import store.server.providers.JsonBodyReader;
import store.server.storage.Procedure;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Manages nodes in the cluster.
 */
public class NodesService {

    private static final Logger LOG = LoggerFactory.getLogger(NodesService.class);
    private final StorageManager storageManager;
    private final NodesDao nodesDao;
    private final NodeNameProvider nodeNameProvider;
    private final PublishUrisProvider publishUrisProvider;
    private final Guid guid;

    /**
     * Constructor.
     *
     * @param storageManager Persistent storage provider.
     * @param nodesDao Nodes definitions DAO.
     * @param attributesDao Attributes DAO.
     * @param nodeNameProvider Node name provider.
     * @param publishUrisProvider Publish URI(s) provider.
     */
    public NodesService(StorageManager storageManager,
                        NodesDao nodesDao,
                        final AttributesDao attributesDao,
                        NodeNameProvider nodeNameProvider,
                        PublishUrisProvider publishUrisProvider) {

        this.storageManager = storageManager;
        this.nodesDao = nodesDao;
        this.nodeNameProvider = nodeNameProvider;
        this.publishUrisProvider = publishUrisProvider;
        this.guid = this.storageManager.inTransaction(new Query<Guid>() {
            @Override
            public Guid apply() {
                return attributesDao.guid();
            }
        });
    }

    /**
     * @return The definition of the local node.
     */
    public NodeDef getNodeDef() {
        return new NodeDef(nodeNameProvider.name(), guid, publishUrisProvider.uris());
    }

    /**
     * Save supplied node definition if:<br>
     * - It is not the local node one.<br>
     * - It is not already tracked.<br>
     * - Associated node is reachable.
     * <p>
     * If any of theses conditions does not hold, does nothing.
     *
     * @param def Node definition to save
     */
    public void saveRemote(NodeDef def) {
        if (def.getGuid().equals(guid) || isAlreadyStored(def)) {
            return;
        }
        final Optional<NodeInfo> info = ping(def.getPublishUris(), def.getGuid());
        if (info.isPresent()) {
            LOG.info("Saving remote node {}", info.get().getDef().getName());
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    nodesDao.saveNodeInfo(info.get());
                }
            });
        }
    }

    private boolean isAlreadyStored(final NodeDef def) {
        return storageManager.inTransaction(new Query<Boolean>() {
            @Override
            public Boolean apply() {
                return nodesDao.containsNodeInfo(def.getGuid());
            }
        });
    }

    /**
     * Add a remote node to tracked ones. Fails if remote node is not reachable, is already tracked or its GUID is the
     * same as the local one.
     *
     * @param uris URIs of the remote node.
     */
    public void addRemote(List<URI> uris) {
        final Optional<NodeInfo> info = ping(uris, null);
        if (info.isPresent()) {
            if (info.get().getDef().getGuid().equals(guid)) {
                throw new SelfTrackingException();
            }
            LOG.info("Adding remote node {}", info.get().getDef().getName());
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    nodesDao.createNodeInfo(info.get());
                }
            });
            return;
        }
        throw new UnreachableNodeException();
    }

    private static Optional<NodeInfo> ping(List<URI> uris, Guid expected) {
        for (URI address : uris) {
            Optional<NodeInfo> info = ping(address);
            if (info.isPresent() && (expected == null || info.get().getDef().getGuid().equals(expected))) {
                return info;
            }
        }
        return Optional.absent();
    }

    private static Optional<NodeInfo> ping(URI uri) {
        ClientConfig clientConfig = new ClientConfig(JsonBodyReader.class);
        Client client = ClientBuilder.newClient(clientConfig);
        try {
            JsonObject json = client.target(uri)
                    .request()
                    .get()
                    .readEntity(JsonObject.class);

            return Optional.of(new NodeInfo(read(json, NodeDef.class), uri, now()));

        } catch (ProcessingException e) {
            return Optional.absent();

        } finally {
            client.close();
        }
    }

    /**
     * Stops tracking a remote node.
     *
     * @param key Node name or encoded GUID.
     */
    public void removeRemote(final String key) {
        LOG.info("Removing remote node {}", key);
        storageManager.inTransaction(new Procedure() {
            @Override
            public void apply() {
                nodesDao.deleteNodeInfo(key);
            }
        });
    }

    /**
     * Loads all remote nodes infos.
     *
     * @return A list of NodeInfo instances.
     */
    public List<NodeInfo> listRemotes() {
        return storageManager.inTransaction(new Query<List<NodeInfo>>() {
            @Override
            public List<NodeInfo> apply() {
                return nodesDao.listNodeInfos(Predicates.<NodeInfo>alwaysTrue());
            }
        });
    }

    /**
     * Loads all remote nodes infos.
     *
     * @return A list of NodeInfo instances.
     */
    public List<NodeInfo> listReachableRemotes() {
        return storageManager.inTransaction(new Query<List<NodeInfo>>() {
            @Override
            public List<NodeInfo> apply() {
                return nodesDao.listNodeInfos(new Predicate<NodeInfo>() {
                    @Override
                    public boolean apply(NodeInfo info) {
                        return info.isReachable();
                    }
                });
            }
        });
    }
}
