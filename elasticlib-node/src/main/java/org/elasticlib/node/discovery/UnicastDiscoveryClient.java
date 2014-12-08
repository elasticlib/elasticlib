package org.elasticlib.node.discovery;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.stream.Collectors.toSet;
import javax.ws.rs.ProcessingException;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.config.Config;
import org.elasticlib.common.config.ConfigException;
import static org.elasticlib.common.config.ConfigUtil.duration;
import static org.elasticlib.common.config.ConfigUtil.unit;
import static org.elasticlib.common.config.ConfigUtil.uris;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.node.config.NodeConfig;
import org.elasticlib.node.manager.task.Task;
import org.elasticlib.node.manager.task.TaskManager;
import org.elasticlib.node.service.ClientLoggingHandler;
import org.elasticlib.node.service.NodesService;
import org.elasticlib.node.service.ProcessingExceptionHandler;
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
    private static final ClientLoggingHandler LOGGING_HANDLER = new ClientLoggingHandler(LOG);
    private static final ProcessingExceptionHandler EXCEPTION_HANDLER = new ProcessingExceptionHandler(LOG);

    private final Config config;
    private final TaskManager taskManager;
    private final NodesService nodesService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Task task;

    /**
     * Constructor.
     *
     * @param config Config.
     * @param taskManager Asynchronous tasks manager.
     * @param nodesService The nodes service.
     */
    public UnicastDiscoveryClient(Config config, TaskManager taskManager, NodesService nodesService) {
        this.config = config;
        this.taskManager = taskManager;
        this.nodesService = nodesService;
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

    private static Set<Guid> guids(List<NodeInfo> infos) {
        return infos.stream()
                .map(info -> info.getDef().getGuid())
                .collect(toSet());
    }

    /**
     * Discovery task.
     */
    private class DiscoveryTask implements Runnable {

        private NodeDef local;
        private List<NodeInfo> remotes;
        private Set<Guid> knownNodes;

        @Override
        public void run() {
            local = nodesService.getNodeDef();
            remotes = nodesService.listReachableRemotes();
            knownNodes = guids(remotes);
            for (URI uri : targetUris()) {
                if (!started.get()) {
                    return;
                }
                process(uri);
            }
        }

        private Iterable<URI> targetUris() {
            if (config.containsKey(NodeConfig.DISCOVERY_UNICAST_URIS)) {
                try {
                    return uris(config, NodeConfig.DISCOVERY_UNICAST_URIS);

                } catch (ConfigException e) {
                    LOG.warn("Config error, using default value", e);
                }
            }
            return Lists.transform(remotes, NodeInfo::getTransportUri);
        }

        private void process(URI target) {
            try (Client client = new Client(target, LOGGING_HANDLER)) {
                List<NodeInfo> targetRemotes = client.remotes().listInfos();
                targetRemotes.stream()
                        .filter(remote -> !knownNodes.contains(remote.getDef().getGuid()))
                        .forEach(remote -> nodesService.saveRemote(remote.getDef()));

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
