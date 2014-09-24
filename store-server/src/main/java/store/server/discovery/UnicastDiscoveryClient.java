package store.server.discovery;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.net.URI;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.client.Client;
import store.common.client.RequestFailedException;
import store.common.config.Config;
import store.common.config.ConfigException;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import static store.common.config.ConfigUtil.uris;
import store.common.hash.Guid;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;
import store.server.config.ServerConfig;
import store.server.manager.task.Task;
import store.server.manager.task.TaskManager;
import store.server.service.ClientLoggingHandler;
import store.server.service.NodesService;
import store.server.service.ProcessingExceptionHandler;

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
        if (!config.getBoolean(ServerConfig.DISCOVERY_UNICAST_ENABLED)) {
            LOG.info("Unicast discovery is disabled");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        task = taskManager.schedule(duration(config, ServerConfig.DISCOVERY_UNICAST_INTERVAL),
                                    unit(config, ServerConfig.DISCOVERY_UNICAST_INTERVAL),
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
        Set<Guid> guids = new HashSet<>();
        for (NodeInfo info : infos) {
            guids.add(info.getDef().getGuid());
        }
        return unmodifiableSet(guids);
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
            if (config.containsKey(ServerConfig.DISCOVERY_UNICAST_URIS)) {
                try {
                    return uris(config, ServerConfig.DISCOVERY_UNICAST_URIS);

                } catch (ConfigException e) {
                    LOG.warn("Config error, using default value", e);
                }
            }
            return Lists.transform(remotes, new Function<NodeInfo, URI>() {
                @Override
                public URI apply(NodeInfo info) {
                    return info.getTransportUri();
                }
            });
        }

        private void process(URI target) {
            try (Client client = new Client(target, LOGGING_HANDLER)) {
                List<NodeInfo> targetRemotes = client.remotes().listInfos();
                for (NodeInfo remote : targetRemotes) {
                    NodeDef def = remote.getDef();
                    if (!knownNodes.contains(def.getGuid())) {
                        nodesService.saveRemote(def);
                    }
                }
                if (!guids(targetRemotes).contains(local.getGuid()) && !local.getPublishUris().isEmpty()) {
                    try {
                        client.remotes().add(local.getPublishUris());

                    } catch (RequestFailedException e) {
                        LOG.warn("Failed to add local node to {}. Remote responded: {}", target, e.getMessage());
                    }
                }
            } catch (ProcessingException e) {
                EXCEPTION_HANDLER.log(target, e);
            }
        }
    }
}
