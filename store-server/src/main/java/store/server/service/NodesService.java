package store.server.service;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import java.net.URI;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.NodeDef;
import store.common.NodeInfo;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.common.hash.Guid;
import store.server.async.AsyncManager;
import store.server.async.Task;
import store.server.config.ServerConfig;
import store.server.dao.AttributesDao;
import store.server.dao.NodesDao;
import store.server.exception.SelfTrackingException;
import store.server.exception.UnreachableNodeException;
import store.server.storage.Procedure;
import store.server.storage.Query;
import store.server.storage.StorageManager;

/**
 * Manages nodes in the cluster.
 */
public class NodesService {

    private static final Logger LOG = LoggerFactory.getLogger(NodesService.class);
    private final Config config;
    private final AsyncManager asyncManager;
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
     * @param asyncManager Asynchronous tasks manager.
     * @param storageManager Persistent storage provider.
     * @param nodesDao Nodes definitions DAO.
     * @param attributesDao Attributes DAO.
     * @param nodeNameProvider Node name provider.
     * @param publishUrisProvider Publish URI(s) provider.
     * @param nodePingHandler remote nodes ping handler.
     */
    public NodesService(Config config,
                        AsyncManager asyncManager,
                        StorageManager storageManager,
                        AttributesDao attributesDao,
                        NodesDao nodesDao,
                        NodeNameProvider nodeNameProvider,
                        PublishUrisProvider publishUrisProvider,
                        NodePingHandler nodePingHandler) {

        this.config = config;
        this.asyncManager = asyncManager;
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
        guid = storageManager.inTransaction(new Query<Guid>() {
            @Override
            public Guid apply() {
                return attributesDao.guid();
            }
        });
        if (config.getBoolean(ServerConfig.REMOTES_PING_ENABLED)) {
            pingTask = asyncManager.schedule(duration(config, ServerConfig.REMOTES_PING_INTERVAL),
                                             unit(config, ServerConfig.REMOTES_PING_INTERVAL),
                                             "Pinging remote nodes",
                                             new PingTask());
        }
        if (config.getBoolean(ServerConfig.REMOTES_CLEANUP_ENABLED)) {
            cleanupTask = asyncManager.schedule(duration(config, ServerConfig.REMOTES_CLEANUP_INTERVAL),
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
        final Optional<NodeInfo> info = nodePingHandler.ping(def.getPublishUris(), def.getGuid());
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
        final Optional<NodeInfo> info = nodePingHandler.ping(uris);
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
     * Loads info of all reachable remote nodes.
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

    /**
     * Ping remote nodes and refresh info about them.
     */
    private class PingTask implements Runnable {

        @Override
        public void run() {
            for (final NodeInfo current : listRemotes()) {
                if (!started.get()) {
                    return;
                }
                final Optional<NodeInfo> updated = nodePingHandler.ping(uris(current), current.getDef().getGuid());
                storageManager.inTransaction(new Procedure() {
                    @Override
                    public void apply() {
                        if (updated.isPresent()) {
                            nodesDao.saveNodeInfo(updated.get());
                        } else {
                            nodesDao.saveNodeInfo(new NodeInfo(current.getDef(), Instant.now()));
                        }
                    }
                });
            }
        }

        private Iterable<URI> uris(final NodeInfo info) {
            if (!info.isReachable()) {
                return info.getDef().getPublishUris();
            }
            return concat(singleton(info.getTransportUri()),
                          filter(info.getDef().getPublishUris(), new Predicate<URI>() {
                @Override
                public boolean apply(URI uri) {
                    return !uri.equals(info.getTransportUri());
                }
            }));
        }
    }

    /**
     * Remove unreachable remote nodes.
     */
    private class CleanupTask implements Runnable {

        @Override
        public void run() {
            storageManager.inTransaction(new Procedure() {
                @Override
                public void apply() {
                    nodesDao.deleteUnreachableNodeInfos();
                }
            });
        }
    }
}
