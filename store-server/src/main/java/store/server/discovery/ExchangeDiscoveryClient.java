package store.server.discovery;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import static com.google.common.collect.Iterables.concat;
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

/**
 * Remote nodes exchange discovery client. Contacts periodically all known remote nodes in order to :<br>
 * - Collects their own remote nodes and register unknown ones among them.<br>
 * - Send them the local node if applicable.
 */
public class ExchangeDiscoveryClient {

    private static final Logger LOG = LoggerFactory.getLogger(ExchangeDiscoveryClient.class);
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
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        task.cancel();
    }

    private static Set<Guid> guids(List<NodeDef> defs) {
        Set<Guid> guids = new HashSet<>();
        for (NodeDef def : defs) {
            guids.add(def.getGuid());
        }
        return unmodifiableSet(guids);
    }

    /**
     * Node exchange task.
     */
    private class ExchangeTask implements Runnable {

        @Override
        public void run() {
            NodeDef local = nodesService.getNodeDef();
            List<NodeDef> remotes = nodesService.listRemotes();
            Set<Guid> knownNodes = guids(remotes);
            for (NodeDef remote : remotes) {
                if (!started.get()) {
                    return;
                }
                new SingleNodeExchangeTask(knownNodes, local, remote).run();
            }
        }
    }

    private class SingleNodeExchangeTask implements Runnable {

        private static final String REMOTES = "remotes";
        private final Set<Guid> knownNodes;
        private final NodeDef localNodeDef;
        private final NodeDef remoteNodeDef;
        private Optional<URI> preferedUri = Optional.absent();

        public SingleNodeExchangeTask(Set<Guid> knownNodes, NodeDef localNodeDef, NodeDef remoteNodeDef) {
            this.knownNodes = knownNodes;
            this.localNodeDef = localNodeDef;
            this.remoteNodeDef = remoteNodeDef;
        }

        @Override
        public void run() {
            Optional<List<NodeDef>> remotesOpt = listRemoteNodes();
            if (!remotesOpt.isPresent()) {
                return;
            }
            List<NodeDef> remotes = remotesOpt.get();
            for (NodeDef remote : remotes) {
                if (!started.get()) {
                    return;
                }
                if (!knownNodes.contains(remote.getGuid())) {
                    nodesService.saveRemote(remote);
                }
            }
            if (!guids(remotes).contains(localNodeDef.getGuid())) {
                addLocalNode();
            }
        }

        private Optional<List<NodeDef>> listRemoteNodes() {
            return request(new Function<WebTarget, List<NodeDef>>() {
                @Override
                public List<NodeDef> apply(WebTarget target) {
                    JsonArray array = target.path(REMOTES)
                            .request()
                            .get()
                            .readEntity(JsonArray.class);

                    return readAll(array, NodeDef.class);
                }
            });
        }

        private void addLocalNode() {
            request(new Function<WebTarget, Void>() {
                @Override
                public Void apply(WebTarget target) {
                    JsonArrayBuilder uris = createArrayBuilder();
                    for (URI uri : localNodeDef.getUris()) {
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

        private <T> Optional<T> request(Function<WebTarget, T> function) {
            ClientConfig clientConfig = new ClientConfig(JsonBodyReader.class, JsonBodyWriter.class);
            Client client = ClientBuilder.newClient(clientConfig);
            try {
                for (URI uri : concat(preferedUri.asSet(), remoteNodeDef.getUris())) {
                    if (!started.get()) {
                        break;
                    }
                    try {
                        T result = function.apply(client.target(uri));
                        preferedUri = Optional.of(uri);
                        return Optional.fromNullable(result);

                    } catch (ProcessingException e) {
                        // Ignore it and try with another URI.
                    }
                }
                return Optional.absent();

            } finally {
                client.close();
            }
        }
    }
}
