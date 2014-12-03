package store.server.service;

import com.google.common.base.Optional;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import java.net.URI;
import static java.time.Instant.now;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.common.exception.SelfTrackingException;
import store.common.exception.UnreachableNodeException;
import store.common.hash.Guid;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.server.config.ServerConfig;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.manager.storage.StorageManager;
import store.server.manager.task.Task;
import store.server.manager.task.TaskManager;

/**
 * Manages nodes in the cluster.
 */
public class NodesService {

    private static final Logger LOG = LoggerFactory.getLogger(NodesService.class);

    private final Config config;
    private final TaskManager taskManager;
    private final StorageManager storageManager;
    private final AttributesDao attributesDao;
    private final NodesDao nodesDao;
    private final NodeNameProvider nodeNameProvider;
    private final PublishUrisProvider publishUrisProvider;
    private final NodePingHandler nodePingHandler;
    private final AtomicBoolean started = new AtomicBoolean();
    private Guid guid;
    private Task pingTask;
    private Task cleanupTask;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param storageManager Persistent storage provider.
     * @param nodesDao Nodes definitions DAO.
     * @param attributesDao Attributes DAO.
     * @param nodeNameProvider Node name provider.
     * @param publishUrisProvider Publish URI(s) provider.
     * @param nodePingHandler remote nodes ping handler.
     */
    public NodesService(Config config,
                        TaskManager taskManager,
                        StorageManager storageManager,
                        AttributesDao attributesDao,
                        NodesDao nodesDao,
                        NodeNameProvider nodeNameProvider,
                        PublishUrisProvider publishUrisProvider,
                        NodePingHandler nodePingHandler) {

        this.config = config;
        this.taskManager = taskManager;
        this.storageManager = storageManager;
        this.attributesDao = attributesDao;
        this.nodesDao = nodesDao;
        this.nodeNameProvider = nodeNameProvider;
        this.publishUrisProvider = publishUrisProvider;
        this.nodePingHandler = nodePingHandler;
    }

    /**
     * Starts this service.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        guid = storageManager.inTransaction(attributesDao::guid);

        if (config.getBoolean(ServerConfig.REMOTES_PING_ENABLED)) {
            pingTask = taskManager.schedule(duration(config, ServerConfig.REMOTES_PING_INTERVAL),
                                            unit(config, ServerConfig.REMOTES_PING_INTERVAL),
                                            "Pinging remote nodes",
                                            new PingTask());
        }
        if (config.getBoolean(ServerConfig.REMOTES_CLEANUP_ENABLED)) {
            cleanupTask = taskManager.schedule(duration(config, ServerConfig.REMOTES_CLEANUP_INTERVAL),
                                               unit(config, ServerConfig.REMOTES_CLEANUP_INTERVAL),
                                               "Removing unreachable remote nodes",
                                               new CleanupTask());
        }
    }

    /**
     * Properly stops this service.
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (pingTask != null) {
            pingTask.cancel();
        }
        if (cleanupTask != null) {
            cleanupTask.cancel();
        }
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
        Optional<NodeInfo> info = nodePingHandler.ping(def.getPublishUris(), def.getGuid());
        if (info.isPresent()) {
            LOG.info("Saving remote node {}", info.get().getDef().getName());
            storageManager.inTransaction(() -> nodesDao.saveNodeInfo(info.get()));
        }
    }

    private boolean isAlreadyStored(NodeDef def) {
        return storageManager.inTransaction(() -> nodesDao.containsNodeInfo(def.getGuid()));
    }

    /**
     * Add a remote node to tracked ones. Fails if remote node is not reachable, is already tracked or its GUID is the
     * same as the local one.
     *
     * @param uris URIs of the remote node.
     */
    public void addRemote(List<URI> uris) {
        Optional<NodeInfo> info = nodePingHandler.ping(uris);
        if (info.isPresent()) {
            if (info.get().getDef().getGuid().equals(guid)) {
                throw new SelfTrackingException();
            }
            LOG.info("Adding remote node {}", info.get().getDef().getName());
            storageManager.inTransaction(() -> nodesDao.createNodeInfo(info.get()));
            return;
        }
        throw new UnreachableNodeException();
    }

    /**
     * Stops tracking a remote node.
     *
     * @param key Node name or encoded GUID.
     */
    public void removeRemote(String key) {
        LOG.info("Removing remote node {}", key);
        storageManager.inTransaction(() -> nodesDao.deleteNodeInfo(key));
    }

    /**
     * Loads all remote nodes infos.
     *
     * @return A list of NodeInfo instances.
     */
    public List<NodeInfo> listRemotes() {
        return storageManager.inTransaction(() -> nodesDao.listNodeInfos(x -> true));
    }

    /**
     * Loads info of all reachable remote nodes.
     *
     * @return A list of NodeInfo instances.
     */
    public List<NodeInfo> listReachableRemotes() {
        return storageManager.inTransaction(() -> nodesDao.listNodeInfos(NodeInfo::isReachable));
    }

    /**
     * Ping remote nodes and refresh info about them.
     */
    private class PingTask implements Runnable {

        @Override
        public void run() {
            for (NodeInfo current : listRemotes()) {
                if (!started.get()) {
                    return;
                }
                Optional<NodeInfo> updated = nodePingHandler.ping(uris(current), current.getDef().getGuid());
                storageManager.inTransaction(() -> {
                    if (updated.isPresent()) {
                        nodesDao.saveNodeInfo(updated.get());
                    } else {
                        nodesDao.saveNodeInfo(new NodeInfo(current.getDef(), now()));
                    }
                });
            }
        }

        private Iterable<URI> uris(NodeInfo info) {
            if (!info.isReachable()) {
                return info.getDef().getPublishUris();
            }
            return concat(singleton(info.getTransportUri()),
                          filter(info.getDef().getPublishUris(), uri -> !uri.equals(info.getTransportUri())));
        }
    }

    /**
     * Remove unreachable remote nodes.
     */
    private class CleanupTask implements Runnable {

        @Override
        public void run() {
            storageManager.inTransaction(nodesDao::deleteUnreachableNodeInfos);
        }
    }
}
