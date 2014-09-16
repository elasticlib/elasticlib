package store.server.discovery;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import java.net.URI;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import static javax.ws.rs.client.Entity.json;
import javax.ws.rs.client.WebTarget;
import org.glassfish.jersey.client.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.NodeDef;
import store.common.NodeInfo;
import store.common.config.Config;
import static store.common.config.ConfigUtil.duration;
import static store.common.config.ConfigUtil.unit;
import store.common.hash.Guid;
import static store.common.json.JsonReading.readAll;
import store.server.async.AsyncManager;
import store.server.async.Task;
import store.server.config.ServerConfig;
import store.server.providers.JsonBodyReader;
import store.server.providers.JsonBodyWriter;
import store.server.service.NodesService;
import store.server.service.ProcessingExceptionHandler;

/**
 * Remote nodes exchange discovery client. Contacts periodically all known remote nodes in order to :<br>
 * - Collects their own remote nodes and register unknown ones among them.<br>
 * - Send them the local node if applicable.
 */
public class ExchangeDiscoveryClient {

    private static final Logger LOG = LoggerFactory.getLogger(ExchangeDiscoveryClient.class);
    private static final ProcessingExceptionHandler HANDLER = new ProcessingExceptionHandler(LOG);
    private final Config config;
    private final AsyncManager asyncManager;
    private final NodesService nodesService;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Task task;

    /**
     * Constructor.
     *
     * @param config Config.
     * @param asyncManager Asynchronous tasks manager.
     * @param nodesService The nodes service.
     */
    public ExchangeDiscoveryClient(Config config, AsyncManager asyncManager, NodesService nodesService) {
        this.config = config;
        this.asyncManager = asyncManager;
        this.nodesService = nodesService;
    }

    /**
     * Starts the client.
     */
    public void start() {
        if (!config.getBoolean(ServerConfig.DISCOVERY_EXCHANGE_ENABLED)) {
            LOG.info("Node exchange discovery is disabled");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        task = asyncManager.schedule(duration(config, ServerConfig.DISCOVERY_EXCHANGE_INTERVAL),
                                     unit(config, ServerConfig.DISCOVERY_EXCHANGE_INTERVAL),
                                     "Exchanging remote nodes",
                                     new ExchangeTask());
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
     * Node exchange task.
     */
    private class ExchangeTask implements Runnable {

        private static final String REMOTES = "remotes";

        @Override
        public void run() {
            NodeDef local = nodesService.getNodeDef();
            List<NodeInfo> remotes = nodesService.listReachableRemotes();
            Set<Guid> knownNodes = guids(remotes);
            for (NodeInfo remote : remotes) {
                if (!started.get()) {
                    return;
                }
                process(knownNodes, local, remote);
            }
        }

        private void process(Set<Guid> knownNodes, NodeDef localNodeDef, NodeInfo remoteNodeInfo) {
            Optional<List<NodeInfo>> remotesOpt = listRemoteNodes(remoteNodeInfo);
            if (!remotesOpt.isPresent()) {
                return;
            }
            List<NodeInfo> remotes = remotesOpt.get();
            for (NodeInfo remote : remotes) {
                NodeDef def = remote.getDef();
                if (!started.get()) {
                    return;
                }
                if (!knownNodes.contains(def.getGuid())) {
                    nodesService.saveRemote(def);
                }
            }
            if (!guids(remotes).contains(localNodeDef.getGuid())) {
                addLocalNode(localNodeDef, remoteNodeInfo);
            }
        }

        private Optional<List<NodeInfo>> listRemoteNodes(NodeInfo node) {
            return request(node, new Function<WebTarget, List<NodeInfo>>() {
                @Override
                public List<NodeInfo> apply(WebTarget target) {
                    JsonArray array = target.path(REMOTES)
                            .request()
                            .get()
                            .readEntity(JsonArray.class);

                    return readAll(array, NodeInfo.class);
                }
            });
        }

        private void addLocalNode(final NodeDef localNodeDef, NodeInfo remoteNodeInfo) {
            request(remoteNodeInfo, new Function<WebTarget, Void>() {
                @Override
                public Void apply(WebTarget target) {
                    JsonArrayBuilder uris = createArrayBuilder();
                    for (URI uri : localNodeDef.getPublishUris()) {
                        uris.add(uri.toString());
                    }
                    JsonObject body = createObjectBuilder()
                            .add("uris", uris)
                            .build();

                    target.path(REMOTES)
                            .request()
                            .post(json(body))
                            .close();

                    return null;
                }
            });
        }

        private <T> Optional<T> request(NodeInfo node, Function<WebTarget, T> function) {
            if (!started.get()) {
                return Optional.absent();
            }
            ClientConfig clientConfig = new ClientConfig(JsonBodyReader.class, JsonBodyWriter.class);
            Client client = ClientBuilder.newClient(clientConfig);
            try {
                return Optional.fromNullable(function.apply(client.target(node.getTransportUri())));

            } catch (ProcessingException e) {
                HANDLER.log(node.getTransportUri(), e);
                return Optional.absent();

            } finally {
                client.close();
            }
        }
    }
}
