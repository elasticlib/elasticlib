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
package org.elasticlib.common.client;

import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Priority;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator;

/**
 * A filter that logs HTTP requests and reponses.
 */
@PreMatching
@Priority(Integer.MIN_VALUE)
public class ClientLoggingFilter implements ClientRequestFilter, ClientResponseFilter {

    private static final String SPACE = " ";
    private static final String DASH = " - ";
    private static final String COLON = ": ";
    private static final String COMMA = ",";
    private static final String REQUEST_PREFIX = "> ";
    private static final String RESPONSE_PREFIX = "< ";
    private static final String ID_PROPERTY = ClientLoggingFilter.class.getName() + ".id";

    private final AtomicLong id;
    private final LoggingHandler handler;

    /**
     * Constructor.
     *
     * @param handler Logging adapter.
     */
    ClientLoggingFilter(LoggingHandler handler) {
        id = new AtomicLong();
        this.handler = handler;
    }

    @Override
    public void filter(ClientRequestContext context) {
        long currentId = id.incrementAndGet();
        context.setProperty(ID_PROPERTY, currentId);

        StringBuilder builder = prefix(new StringBuilder(), currentId, REQUEST_PREFIX)
                .append(context.getMethod())
                .append(SPACE)
                .append(context.getUri().toASCIIString())
                .append(lineSeparator());

        printPrefixedHeaders(builder, currentId, REQUEST_PREFIX, context.getStringHeaders());

        handler.logRequest(builder.toString());
    }

    @Override
    public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) {
        long currentId = (long) requestContext.getProperty(ID_PROPERTY);

        StringBuilder builder = prefix(new StringBuilder(), currentId, RESPONSE_PREFIX)
                .append(responseContext.getStatus())
                .append(DASH)
                .append(responseContext.getStatusInfo().getReasonPhrase())
                .append(lineSeparator());

        printPrefixedHeaders(builder, currentId, RESPONSE_PREFIX, responseContext.getHeaders());

        handler.logResponse(builder.toString());
    }

    private static void printPrefixedHeaders(StringBuilder builder,
                                             long id,
                                             String prefix,
                                             MultivaluedMap<String, String> headers) {
        headers.entrySet()
                .stream()
                .sorted((x, y) -> StringIgnoreCaseKeyComparator.SINGLETON.compare(x.getKey(), y.getKey()))
                .forEach(header -> {
                    prefix(builder, id, prefix)
                    .append(header.getKey())
                    .append(COLON)
                    .append(join(COMMA, header.getValue()))
                    .append(lineSeparator());
                });
    }

    private static StringBuilder prefix(StringBuilder builder, long id, String prefix) {
        return builder.append(Long.toString(id))
                .append(SPACE)
                .append(prefix);
    }
}
