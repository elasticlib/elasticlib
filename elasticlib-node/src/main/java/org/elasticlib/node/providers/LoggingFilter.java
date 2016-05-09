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
import static java.lang.System.lineSeparator;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import org.elasticlib.common.util.HttpLogBuilder;
import org.elasticlib.common.util.IdProvider;
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

    private static final String ENTITY_LOGGER_PROPERTY = LoggingFilter.class.getName() + ".entityLogger";
    private static final String ID_PROPERTY = LoggingFilter.class.getName() + ".id";
    private static final int MAX_ENTITY_SIZE = 8 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFilter.class);

    private final IdProvider idProvider;

    /**
     * Constructor.
     *
     * @param idProvider Provides identifiers for logged requests and responses.
     */
    public LoggingFilter(IdProvider idProvider) {
        this.idProvider = idProvider;
    }

    @Override
    public void filter(ContainerRequestContext requestCtx) throws IOException {
        long currentId = idProvider.get();
        requestCtx.setProperty(ID_PROPERTY, currentId);

        String message = new HttpLogBuilder(currentId).request(requestCtx.getMethod(),
                                                               requestCtx.getUriInfo().getRequestUri().toASCIIString(),
                                                               requestCtx.getHeaders());

        LOG.info("Received request{}{}", lineSeparator(), message);

        if (requestCtx.hasEntity() && LOG.isDebugEnabled()) {
            StringBuilder entityBuilder = new StringBuilder();
            requestCtx.setEntityStream(logInboundEntity(currentId, entityBuilder, requestCtx.getEntityStream()));
            LOG.debug(entityBuilder.toString());
        }
    }

    @Override
    public void filter(ContainerRequestContext requestCtx, ContainerResponseContext responseCtx) throws IOException {
        long currentId = (long) requestCtx.getProperty(ID_PROPERTY);

        String message = new HttpLogBuilder(currentId).response(responseCtx.getStatus(),
                                                                responseCtx.getStatusInfo().getReasonPhrase(),
                                                                responseCtx.getStringHeaders());

        LOG.info("Responded with response{}{}", lineSeparator(), message);

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
            LOG.debug(stream.toString());
        }
    }

    private static InputStream logInboundEntity(long id, StringBuilder builder, InputStream stream) throws IOException {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(MAX_ENTITY_SIZE + 1);
        byte[] entity = new byte[MAX_ENTITY_SIZE + 1];
        int entitySize = stream.read(entity);
        boolean hasMore = false;
        if (entitySize < 0 || entitySize > MAX_ENTITY_SIZE) {
            entitySize = MAX_ENTITY_SIZE;
            hasMore = true;
        }

        builder.append(new HttpLogBuilder(id).requestEntity(entity, entitySize, hasMore));
        stream.reset();
        return stream;
    }

    /**
     * An output stream that logs outbound entity.
     */
    private static class LoggingStream extends OutputStream {

        private final long id;
        private final OutputStream entityStream;
        private final ByteArrayOutputStream buffer;
        private int entitySize;

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
         * Writes entity to a string.
         *
         * @return A new String instance.
         */
        @Override
        public String toString() {
            byte[] entity = buffer.toByteArray();
            return new HttpLogBuilder(id).responseEntity(entity, entity.length, entitySize > MAX_ENTITY_SIZE);
        }

        @Override
        public void write(int i) throws IOException {
            if (buffer.size() <= MAX_ENTITY_SIZE) {
                buffer.write(i);
            }
            entityStream.write(i);
            entitySize++;
        }
    }
}
