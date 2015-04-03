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
package org.elasticlib.node.discovery;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import javax.ws.rs.ProcessingException;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.config.ConfigUtil;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.RemoteInfo;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.manager.client.ClientsManager;
import org.elasticlib.node.manager.client.ProcessingExceptionHandler;
import org.elasticlib.node.manager.task.Task;
import org.elasticlib.node.manager.task.TaskManager;
import org.elasticlib.node.service.NodeService;
import org.elasticlib.node.service.RemotesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unicast discovery client. Contacts periodically one or several remote nodes in order to :<br>
 * - Collects their own remote nodes and register unknown ones among them.<br>
 * - Send them the local node if applicable.
 * <p>
 * Node(s) to contact may be statically supplied by URI in the configuration. If they are not specified by this mean,
 * all known remotes notes are contacted.
 */
public class UnicastDiscoveryClient {

    private static final Logger LOG = LoggerFactory.getLogger(UnicastDiscoveryClient.class);
    private static final ProcessingExceptionHandler EXCEPTION_HANDLER = new ProcessingExceptionHandler(LOG);

    private final Config config;
    private final ClientsManager clientsManager;
    private final TaskManager taskManager;
    private final NodeService nodeService;
    private final RemotesService remotesService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Task task;

    /**
     * Constructor.
     *
     * @param config Config.
     * @param clientsManager Node clients manager.
     * @param taskManager Asynchronous tasks manager.
     * @param nodeService Local node service.
     * @param remotesService Remote nodes service.
     */
    public UnicastDiscoveryClient(Config config,
                                  ClientsManager clientsManager,
                                  TaskManager taskManager,
                                  NodeService nodeService,
                                  RemotesService remotesService) {
        this.config = config;
        this.clientsManager = clientsManager;
        this.taskManager = taskManager;
        this.nodeService = nodeService;
        this.remotesService = remotesService;
    }

    /**
     * Starts the client.
     */
    public void start() {
        if (!config.getBoolean(NodeConfig.DISCOVERY_UNICAST_ENABLED)) {
            LOG.info("Unicast discovery is disabled");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        task = taskManager.schedule(duration(config, NodeConfig.DISCOVERY_UNICAST_INTERVAL),
                                    unit(config, NodeConfig.DISCOVERY_UNICAST_INTERVAL),
                                    "Performing unicast discovery",
                                    new DiscoveryTask());
    }

    /**
     * Stops the client, releasing underlying ressources.
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        task.cancel();
    }

    private static Set<Guid> guids(List<RemoteInfo> infos) {
        return infos.stream()
                .map(info -> info.getGuid())
                .collect(toSet());
    }

    /**
     * Discovery task.
     */
    private class DiscoveryTask implements Runnable {

        private NodeDef local;
        private List<RemoteInfo> remotes;
        private Set<Guid> knownNodes;

        @Override
        public void run() {
            local = nodeService.getNodeDef();
            remotes = remotesService.listReachableRemotes();
            knownNodes = guids(remotes);
            for (URI uri : targetUris()) {
                if (!started.get()) {
                    return;
                }
                process(uri);
            }
        }

        private Iterable<URI> targetUris() {
            List<URI> uris = ConfigUtil.uris(config, NodeConfig.DISCOVERY_UNICAST_URIS);
            if (!uris.isEmpty()) {
                return uris;
            }
            return remotes.stream()
                    .map(RemoteInfo::getTransportUri)
                    .collect(toList());
        }

        private void process(URI target) {
            try (Client client = clientsManager.getClient(target)) {
                if (remotes.stream().noneMatch(x -> x.getTransportUri().equals(target))) {
                    remotesService.saveRemote(target);
                }

                List<RemoteInfo> targetRemotes = client.remotes().listInfos();
                targetRemotes.stream()
                        .filter(remote -> !knownNodes.contains(remote.getGuid()))
                        .forEach(remote -> remotesService.saveRemote(remote.getPublishUris(), remote.getGuid()));

                if (!guids(targetRemotes).contains(local.getGuid()) && !local.getPublishUris().isEmpty()) {
                    try {
                        client.remotes().add(local.getPublishUris());

                    } catch (NodeException e) {
                        LOG.warn("Failed to add local node to {}. Remote responded: {}", target, e.getMessage());
                    }
                }
            } catch (ProcessingException e) {
                EXCEPTION_HANDLER.log(target, e);
            }
        }
    }
}
