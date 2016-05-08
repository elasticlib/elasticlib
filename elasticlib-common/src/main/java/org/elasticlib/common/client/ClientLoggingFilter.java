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

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Priority;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.container.PreMatching;
import org.elasticlib.common.util.HttpLogBuilder;

/**
 * A filter that logs HTTP requests and reponses.
 */
@PreMatching
@Priority(Integer.MIN_VALUE)
public class ClientLoggingFilter implements ClientRequestFilter, ClientResponseFilter {

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

        String message = new HttpLogBuilder(currentId).request(context.getMethod(),
                                                               context.getUri().toASCIIString(),
                                                               context.getStringHeaders());

        handler.logRequest(message);
    }

    @Override
    public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) {
        long currentId = (long) requestContext.getProperty(ID_PROPERTY);
        String message = new HttpLogBuilder(currentId).response(responseContext.getStatus(),
                                                                responseContext.getStatusInfo().getReasonPhrase(),
                                                                responseContext.getHeaders());

        handler.logResponse(message);
    }
}
