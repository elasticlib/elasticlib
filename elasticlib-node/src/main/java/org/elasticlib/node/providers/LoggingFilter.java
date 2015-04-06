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
package org.elasticlib.node.providers;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import static java.lang.Math.min;
import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.StatusType;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side HTTP logging filter.
 * <p>
 * Logs requests and responses lines with associated headers at INFO level. Entities are logged at DEBUG level.
 */
@PreMatching
@Priority(Integer.MIN_VALUE)
public class LoggingFilter implements ContainerRequestFilter, ContainerResponseFilter, WriterInterceptor {

    private static final String SPACE = " ";
    private static final String DASH = " - ";
    private static final String COLON = ": ";
    private static final String COMMA = ",";
    private static final String NOTIFICATION_PREFIX = "* ";
    private static final String REQUEST_PREFIX = "> ";
    private static final String RESPONSE_PREFIX = "< ";
    private static final String MORE = "...more...";
    private static final String REQUEST_ENTITY_NOTE = "With request entity";
    private static final String RESPONSE_ENTITY_NOTE = "With response entity";
    private static final String REQUEST_NOTE = "Server has received a request";
    private static final String RESPONSE_NOTE = "Server responded with a response";
    private static final String ENTITY_LOGGER_PROPERTY = LoggingFilter.class.getName() + ".entityLogger";
    private static final String ID_PROPERTY = LoggingFilter.class.getName() + ".id";
    private static final int MAX_ENTITY_SIZE = 8 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFilter.class);

    private final AtomicLong id = new AtomicLong();

    @Override
    public void filter(ContainerRequestContext requestCtx) throws IOException {
        long currentId = id.incrementAndGet();
        requestCtx.setProperty(ID_PROPERTY, currentId);
        StringBuilder builder = new StringBuilder();

        printRequestLine(builder,
                         REQUEST_NOTE,
                         currentId,
                         requestCtx.getMethod(),
                         requestCtx.getUriInfo().getRequestUri());

        printPrefixedHeaders(builder,
                             currentId,
                             REQUEST_PREFIX,
                             requestCtx.getHeaders());

        LOG.info(builder.toString());

        if (requestCtx.hasEntity() && LOG.isDebugEnabled()) {
            StringBuilder entityBuilder = new StringBuilder();
            requestCtx.setEntityStream(logInboundEntity(currentId, entityBuilder, requestCtx.getEntityStream()));
            LOG.debug(entityBuilder.toString());
        }
    }

    @Override
    public void filter(ContainerRequestContext requestCtx, ContainerResponseContext responseCtx) throws IOException {
        long currentId = (long) requestCtx.getProperty(ID_PROPERTY);
        StringBuilder builder = new StringBuilder();

        printResponseLine(builder,
                          RESPONSE_NOTE,
                          currentId,
                          responseCtx.getStatusInfo());

        printPrefixedHeaders(builder,
                             currentId,
                             RESPONSE_PREFIX,
                             responseCtx.getStringHeaders());

        LOG.info(builder.toString());

        if (responseCtx.hasEntity() && LOG.isDebugEnabled()) {
            OutputStream stream = new LoggingStream(currentId, responseCtx.getEntityStream());
            responseCtx.setEntityStream(stream);
            requestCtx.setProperty(ENTITY_LOGGER_PROPERTY, stream);
            // The interceptor will log the responded entity.
        }
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext writerInterceptorCtx) throws IOException {
        LoggingStream stream = (LoggingStream) writerInterceptorCtx.getProperty(ENTITY_LOGGER_PROPERTY);
        writerInterceptorCtx.proceed();
        if (stream != null && LOG.isDebugEnabled()) {
            LOG.debug(stream.getStringBuilder().toString());
        }
    }

    private static void printRequestLine(StringBuilder builder, String note, long id, String method, URI uri) {
        prefixId(builder, id)
                .append(NOTIFICATION_PREFIX)
                .append(note)
                .append(lineSeparator());

        prefixId(builder, id)
                .append(REQUEST_PREFIX)
                .append(method)
                .append(SPACE)
                .append(uri.toASCIIString())
                .append(lineSeparator());
    }

    private static void printResponseLine(StringBuilder builder, String note, long id, StatusType status) {
        prefixId(builder, id)
                .append(NOTIFICATION_PREFIX)
                .append(note)
                .append(lineSeparator());

        prefixId(builder, id)
                .append(RESPONSE_PREFIX)
                .append(status.getStatusCode())
                .append(DASH)
                .append(status.getReasonPhrase())
                .append(lineSeparator());
    }

    private static void printPrefixedHeaders(StringBuilder builder,
                                             long id,
                                             String prefix,
                                             MultivaluedMap<String, String> headers) {
        headers.entrySet()
                .stream()
                .sorted((x, y) -> x.getKey().compareToIgnoreCase(y.getKey()))
                .forEach(headerEntry -> {
                    prefixId(builder, id)
                    .append(prefix)
                    .append(headerEntry.getKey())
                    .append(COLON)
                    .append(join(COMMA, headerEntry.getValue()))
                    .append(lineSeparator());
                });
    }

    private static InputStream logInboundEntity(long id, StringBuilder builder, InputStream stream) throws IOException {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(MAX_ENTITY_SIZE + 1);
        byte[] entity = new byte[MAX_ENTITY_SIZE + 1];
        int entitySize = stream.read(entity);

        prefixId(builder, id)
                .append(NOTIFICATION_PREFIX)
                .append(REQUEST_ENTITY_NOTE)
                .append(lineSeparator())
                .append(new String(entity, 0, min(entitySize, MAX_ENTITY_SIZE)));

        if (entitySize > MAX_ENTITY_SIZE) {
            builder.append(MORE);
        }
        builder.append(lineSeparator());
        stream.reset();
        return stream;
    }

    private static StringBuilder prefixId(StringBuilder builder, long id) {
        return builder
                .append(Long.toString(id))
                .append(SPACE);
    }

    /**
     * An output stream that logs outbound entity.
     */
    private static class LoggingStream extends OutputStream {

        private final long id;
        private final OutputStream entityStream;
        private final ByteArrayOutputStream buffer;

        /**
         * Constructor.
         *
         * @param id Response identifier.
         * @param entityStream Actual response entity stream.
         */
        public LoggingStream(long id, OutputStream entityStream) {
            this.id = id;
            this.entityStream = entityStream;
            buffer = new ByteArrayOutputStream();
        }

        /**
         * Writes entity to a string builder and returns filled builder.
         *
         * @return A new StringBuilder instance.
         */
        public StringBuilder getStringBuilder() {
            byte[] entity = buffer.toByteArray();
            StringBuilder builder = prefixId(new StringBuilder(), id)
                    .append(NOTIFICATION_PREFIX)
                    .append(RESPONSE_ENTITY_NOTE)
                    .append(lineSeparator())
                    .append(new String(entity, 0, min(entity.length, MAX_ENTITY_SIZE)));

            if (entity.length > MAX_ENTITY_SIZE) {
                builder.append(MORE);
            }
            builder.append(lineSeparator());
            return builder;
        }

        @Override
        public void write(int i) throws IOException {
            if (buffer.size() <= MAX_ENTITY_SIZE) {
                buffer.write(i);
            }
            entityStream.write(i);
        }
    }
}
