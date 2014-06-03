package store.server.providers;

import com.google.common.base.Joiner;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator;
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
    private static final String NOTIFICATION_PREFIX = "* ";
    private static final String REQUEST_PREFIX = "> ";
    private static final String RESPONSE_PREFIX = "< ";
    private static final String MORE = "...more...";
    private static final String REQUEST_ENTITY_NOTE = "With request entity";
    private static final String RESPONSE_ENTITY_NOTE = "With response entity";
    private static final String REQUEST_NOTE = "Server has received a request";
    private static final String RESPONSE_NOTE = "Server responded with a response";
    private static final String ENTITY_LOGGER_PROPERTY = LoggingFilter.class.getName() + ".entityLogger";
    private static final int MAX_ENTITY_SIZE = 8 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFilter.class);
    private static final Comparator<Entry<String, List<String>>> COMPARATOR =
            new Comparator<Entry<String, List<String>>>() {
        @Override
        public int compare(Entry<String, List<String>> o1, Entry<String, List<String>> o2) {
            return StringIgnoreCaseKeyComparator.SINGLETON.compare(o1.getKey(), o2.getKey());
        }
    };
    private final AtomicLong id = new AtomicLong();

    private StringBuilder prefixId(StringBuilder builder, long id) {
        builder.append(Long.toString(id)).append(SPACE);
        return builder;
    }

    private void printRequestLine(StringBuilder builder, String note, long id, String method, URI uri) {
        prefixId(builder, id)
                .append(NOTIFICATION_PREFIX)
                .append(note)
                .append(System.lineSeparator());

        prefixId(builder, id)
                .append(REQUEST_PREFIX)
                .append(method)
                .append(SPACE)
                .append(uri.toASCIIString())
                .append(System.lineSeparator());
    }

    private void printResponseLine(StringBuilder builder, String note, long id, int status) {
        prefixId(builder, id)
                .append(NOTIFICATION_PREFIX)
                .append(note)
                .append(System.lineSeparator());

        prefixId(builder, id)
                .append(RESPONSE_PREFIX)
                .append(Integer.toString(status))
                .append(System.lineSeparator());
    }

    private void printPrefixedHeaders(StringBuilder builder, long id, String prefix,
                                      MultivaluedMap<String, String> headers) {

        TreeSet<Entry<String, List<String>>> sortedHeaders = new TreeSet<>(COMPARATOR);
        sortedHeaders.addAll(headers.entrySet());

        for (Entry<String, List<String>> headerEntry : sortedHeaders) {
            List<?> val = headerEntry.getValue();
            String header = headerEntry.getKey();

            prefixId(builder, id)
                    .append(prefix)
                    .append(header)
                    .append(": ")
                    .append(Joiner.on(',').join(val))
                    .append(System.lineSeparator());
        }
    }

    private InputStream logInboundEntity(long id, StringBuilder builder, InputStream stream) throws IOException {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(MAX_ENTITY_SIZE + 1);
        byte[] entity = new byte[MAX_ENTITY_SIZE + 1];
        int entitySize = stream.read(entity);

        prefixId(builder, id)
                .append(NOTIFICATION_PREFIX)
                .append(REQUEST_ENTITY_NOTE)
                .append(System.lineSeparator())
                .append(new String(entity, 0, Math.min(entitySize, MAX_ENTITY_SIZE)));

        if (entitySize > MAX_ENTITY_SIZE) {
            builder.append(MORE);
        }
        builder.append(System.lineSeparator());
        stream.reset();
        return stream;
    }

    @Override
    public void filter(ContainerRequestContext requestCtx) throws IOException {
        long currentId = id.incrementAndGet();
        StringBuilder requestBuilder = new StringBuilder();
        StringBuilder entityBuilder = new StringBuilder();

        printRequestLine(requestBuilder,
                         REQUEST_NOTE,
                         currentId,
                         requestCtx.getMethod(),
                         requestCtx.getUriInfo().getRequestUri());

        printPrefixedHeaders(requestBuilder,
                             currentId,
                             REQUEST_PREFIX,
                             requestCtx.getHeaders());

        LOG.info(requestBuilder.toString());
        if (requestCtx.hasEntity()) {
            requestCtx.setEntityStream(logInboundEntity(currentId, entityBuilder, requestCtx.getEntityStream()));
            LOG.debug(entityBuilder.toString());
        }
    }

    @Override
    public void filter(ContainerRequestContext requestCtx, ContainerResponseContext responseCtx) throws IOException {
        long currentId = id.incrementAndGet();
        StringBuilder builder = new StringBuilder();

        printResponseLine(builder,
                          RESPONSE_NOTE,
                          currentId,
                          responseCtx.getStatus());

        printPrefixedHeaders(builder,
                             currentId,
                             RESPONSE_PREFIX,
                             responseCtx.getStringHeaders());

        if (responseCtx.hasEntity()) {
            OutputStream stream = new LoggingStream(currentId, responseCtx.getEntityStream());
            responseCtx.setEntityStream(stream);
            requestCtx.setProperty(ENTITY_LOGGER_PROPERTY, stream);
            // The interceptor will log the responded entity.
        }
        LOG.info(builder.toString());
    }

    @Override
    public void aroundWriteTo(WriterInterceptorContext writerInterceptorCtx) throws IOException {
        LoggingStream stream = (LoggingStream) writerInterceptorCtx.getProperty(ENTITY_LOGGER_PROPERTY);
        writerInterceptorCtx.proceed();
        if (stream != null) {
            LOG.debug(stream.getStringBuilder().toString());
        }
    }

    private class LoggingStream extends OutputStream {

        private final StringBuilder builder;
        private final OutputStream inner;
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        LoggingStream(long id, OutputStream inner) {
            this.builder = prefixId(new StringBuilder(), id)
                    .append(NOTIFICATION_PREFIX)
                    .append(RESPONSE_ENTITY_NOTE)
                    .append(System.lineSeparator());
            this.inner = inner;
        }

        StringBuilder getStringBuilder() {
            // write entity to the builder
            byte[] entity = baos.toByteArray();
            builder.append(new String(entity, 0, Math.min(entity.length, MAX_ENTITY_SIZE)));
            if (entity.length > MAX_ENTITY_SIZE) {
                builder.append(MORE);
            }
            builder.append(System.lineSeparator());
            return builder;
        }

        @Override
        public void write(int i) throws IOException {
            if (baos.size() <= MAX_ENTITY_SIZE) {
                baos.write(i);
            }
            inner.write(i);
        }
    }
}
