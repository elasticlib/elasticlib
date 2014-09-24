package store.server.service;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import java.net.URI;
import javax.ws.rs.ProcessingException;
import static org.joda.time.Instant.now;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.common.client.Client;
import store.common.hash.Guid;
import store.common.model.NodeDef;
import store.common.model.NodeInfo;

/**
 * Remote nodes ping handler.
 */
public class NodePingHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NodePingHandler.class);
    private static final ClientLoggingHandler LOGGING_HANDLER = new ClientLoggingHandler(LOG);
    private static final ProcessingExceptionHandler EXCEPTION_HANDLER = new ProcessingExceptionHandler(LOG);

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
        try (Client client = new Client(uri, LOGGING_HANDLER)) {
            NodeDef def = client.node().getDef();
            return Optional.of(new NodeInfo(def, uri, now()));

        } catch (ProcessingException e) {
            EXCEPTION_HANDLER.log(uri, e);
            return Optional.absent();
        }
    }
}
