package store.server.service;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import java.net.URI;
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
import store.server.providers.JsonBodyReader;

/**
 * Remote nodes ping handler.
 */
public class NodePingHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NodePingHandler.class);

    /**
     * Calls the supplied uris and returns info of the first node that responds.
     *
     * @param uris Some node URI(s).
     * @return Info about the first reachable node.
     */
    public Optional<NodeInfo> ping(Iterable<URI> uris) {
        return ping(uris, Predicates.<NodeDef>alwaysTrue());
    }

    /**
     * Calls the supplied uris and returns info of the first node that responds and which GUID matches expected one.
     *
     * @param uris Some node URI(s).
     * @param expected Expected node GUID.
     * @return Info about the first reachable node which requested GUID.
     */
    public Optional<NodeInfo> ping(Iterable<URI> uris, final Guid expected) {
        return ping(uris, new Predicate<NodeDef>() {
            @Override
            public boolean apply(NodeDef def) {
                return def.getGuid().equals(expected);
            }
        });
    }

    private static Optional<NodeInfo> ping(Iterable<URI> uris, Predicate<NodeDef> predicate) {
        for (URI address : uris) {
            Optional<NodeInfo> info = ping(address);
            if (info.isPresent() && predicate.apply(info.get().getDef())) {
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
            LOG.warn("Failed to resolve local host", e);
            return Optional.absent();

        } finally {
            client.close();
        }
    }
}
