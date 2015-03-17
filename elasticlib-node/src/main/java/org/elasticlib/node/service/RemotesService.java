/*
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.node.service;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import java.net.URI;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.elasticlib.common.config.Config;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import org.elasticlib.common.exception.SelfTrackingException;
import org.elasticlib.common.exception.UnreachableNodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.dao.RemotesDao;
import org.elasticlib.node.manager.storage.StorageManager;
import org.elasticlib.node.manager.task.Task;
import org.elasticlib.node.manager.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages remote nodes in the cluster.
 */
public class RemotesService {

    private static final Logger LOG = LoggerFactory.getLogger(RemotesService.class);

    private final Config config;
    private final TaskManager taskManager;
    private final StorageManager storageManager;
    private final RemotesDao remotesDao;
    private final NodeGuidProvider nodeGuidProvider;
    private final NodePingHandler nodePingHandler;
    private final AtomicBoolean started = new AtomicBoolean();
    private Task pingTask;
    private Task cleanupTask;

    /**
     * Constructor.
     *
     * @param config Configuration holder.
     * @param taskManager Asynchronous tasks manager.
     * @param storageManager Persistent storage provider.
     * @param remotesDao Remote nodes DAO.
     * @param nodeGuidProvider Local node GUID provider.
     * @param nodePingHandler Remote nodes ping handler.
     */
    public RemotesService(Config config,
                          TaskManager taskManager,
                          StorageManager storageManager,
                          RemotesDao remotesDao,
                          NodeGuidProvider nodeGuidProvider,
                          NodePingHandler nodePingHandler) {

        this.config = config;
        this.taskManager = taskManager;
        this.storageManager = storageManager;
        this.remotesDao = remotesDao;
        this.nodeGuidProvider = nodeGuidProvider;
        this.nodePingHandler = nodePingHandler;
    }

    /**
     * Starts this service.
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        if (config.getBoolean(NodeConfig.REMOTES_PING_ENABLED)) {
            pingTask = taskManager.schedule(duration(config, NodeConfig.REMOTES_PING_INTERVAL),
                                            unit(config, NodeConfig.REMOTES_PING_INTERVAL),
                                            "Pinging remote nodes",
                                            this::pingRemotes);
        }
        if (config.getBoolean(NodeConfig.REMOTES_CLEANUP_ENABLED)) {
            cleanupTask = taskManager.schedule(duration(config, NodeConfig.REMOTES_CLEANUP_INTERVAL),
                                               unit(config, NodeConfig.REMOTES_CLEANUP_INTERVAL),
                                               "Removing unreachable remote nodes",
                                               this::cleanupRemotes);
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
     * Save the remote node with supplied URI(s) and GUID if:<br>
     * - It is not the local node one.<br>
     * - It is not already tracked.<br>
     * - Associated node is reachable.
     * <p>
     * If any of theses conditions does not hold, does nothing.
     *
     * @param uris URI(s) of the remote node.
     * @param expected Remote node GUID.
     */
    public void saveRemote(List<URI> uris, Guid expected) {
        if (expected.equals(nodeGuidProvider.guid()) || isAlreadyStored(expected)) {
            return;
        }
        Optional<RemoteInfo> info = nodePingHandler.ping(uris, expected);
        if (info.isPresent()) {
            LOG.info("Saving remote node {}", info.get().getName());
            storageManager.inTransaction(() -> remotesDao.saveRemoteInfo(info.get()));
        }
    }

    private boolean isAlreadyStored(Guid guid) {
        return storageManager.inTransaction(() -> remotesDao.containsRemoteInfo(guid));
    }

    /**
     * Save the remote node with supplied URI if:<br>
     * - It is not the local node one.<br>
     * - It is not already tracked.<br>
     * - It is reachable.
     * <p>
     * If any of theses conditions does not hold, does nothing.
     *
     * @param uri Remote node URI.
     */
    public void saveRemote(URI uri) {
        Optional<RemoteInfo> info = nodePingHandler.ping(uri);
        if (info.isPresent() && !info.get().getGuid().equals(nodeGuidProvider.guid())) {
            LOG.info("Saving remote node {}", info.get().getName());
            storageManager.inTransaction(() -> remotesDao.saveRemoteInfo(info.get()));
        }
    }

    /**
     * Add a remote node to tracked ones. Fails if remote node is not reachable, is already tracked or its GUID is the
     * same as the local one.
     *
     * @param uris URI(s) of the remote node.
     */
    public void addRemote(List<URI> uris) {
        Optional<RemoteInfo> info = nodePingHandler.ping(uris);
        if (info.isPresent()) {
            if (info.get().getGuid().equals(nodeGuidProvider.guid())) {
                throw new SelfTrackingException();
            }
            LOG.info("Adding remote node {}", info.get().getName());
            storageManager.inTransaction(() -> remotesDao.createRemoteInfo(info.get()));
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
        storageManager.inTransaction(() -> remotesDao.deleteRemoteInfo(key));
    }

    /**
     * Loads all remote nodes infos.
     *
     * @return A list of RemoteInfo instances.
     */
    public List<RemoteInfo> listRemotes() {
        LOG.info("Returning remote infos");
        return remoteInfos();
    }

    private List<RemoteInfo> remoteInfos() {
        return storageManager.inTransaction(() -> remotesDao.listRemoteInfos(x -> true));
    }

    /**
     * Loads info of all reachable remote nodes.
     *
     * @return A list of RemoteInfo instances.
     */
    public List<RemoteInfo> listReachableRemotes() {
        LOG.info("Returning reachable remote infos");
        return storageManager.inTransaction(() -> remotesDao.listRemoteInfos(RemoteInfo::isReachable));
    }

    /**
     * Pings all known remote nodes and refresh info about them.
     */
    public void pingRemotes() {
        LOG.info("Pinging remote nodes");
        for (RemoteInfo current : remoteInfos()) {
            if (!started.get()) {
                return;
            }
            Optional<RemoteInfo> updated = nodePingHandler.ping(uris(current), current.getGuid());
            storageManager.inTransaction(() -> {
                if (updated.isPresent()) {
                    remotesDao.saveRemoteInfo(updated.get());
                } else {
                    remotesDao.saveRemoteInfo(current.asUnreachable());
                }
            });
        }
    }

    private static Iterable<URI> uris(RemoteInfo info) {
        if (!info.isReachable()) {
            return info.getPublishUris();
        }
        return concat(singleton(info.getTransportUri()),
                      filter(info.getPublishUris(), uri -> !uri.equals(info.getTransportUri())));
    }

    /**
     * Removes all unreachable remote nodes.
     */
    public void cleanupRemotes() {
        LOG.info("Removing unreachable remote nodes");
        storageManager.inTransaction(remotesDao::deleteUnreachableRemoteInfos);
    }
}
