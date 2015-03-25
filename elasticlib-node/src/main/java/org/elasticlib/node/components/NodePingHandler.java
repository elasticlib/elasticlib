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
package org.elasticlib.node.components;

import java.net.URI;
import static java.time.Instant.now;
import java.util.Optional;
import java.util.function.Predicate;
import javax.ws.rs.ProcessingException;
import org.elasticlib.common.client.Client;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.model.NodeInfo;
import org.elasticlib.common.model.RemoteInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remote nodes ping handler.
 */
public class NodePingHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NodePingHandler.class);
    private static final ClientLoggingHandler LOGGING_HANDLER = new ClientLoggingHandler(LOG);
    private static final ProcessingExceptionHandler EXCEPTION_HANDLER = new ProcessingExceptionHandler(LOG);

    /**
     * Calls the supplied URI and returns info of the associated node if it responds.
     *
     * @param uri A node URI.
     * @return Info about the associated node.
     */
    public Optional<RemoteInfo> ping(URI uri) {
        try (Client client = new Client(uri, LOGGING_HANDLER)) {
            NodeInfo info = client.node().getInfo();
            return Optional.of(new RemoteInfo(info, uri, now()));

        } catch (ProcessingException e) {
            EXCEPTION_HANDLER.log(uri, e);
            return Optional.empty();
        }
    }

    /**
     * Calls the supplied URIs and returns info of the first node that responds.
     *
     * @param uris Some node URI(s).
     * @return Info about the first reachable node.
     */
    public Optional<RemoteInfo> ping(Iterable<URI> uris) {
        return ping(uris, x -> true);
    }

    /**
     * Calls the supplied URIs and returns info of the first node that responds and which GUID matches expected one.
     *
     * @param uris Some node URI(s).
     * @param expected Expected node GUID.
     * @return Info about the first reachable node which requested GUID.
     */
    public Optional<RemoteInfo> ping(Iterable<URI> uris, Guid expected) {
        return ping(uris, x -> x.getGuid().equals(expected));
    }

    private Optional<RemoteInfo> ping(Iterable<URI> uris, Predicate<RemoteInfo> predicate) {
        for (URI address : uris) {
            Optional<RemoteInfo> info = ping(address);
            if (info.isPresent() && predicate.test(info.get())) {
                return info;
            }
        }
        return Optional.empty();
    }
}
