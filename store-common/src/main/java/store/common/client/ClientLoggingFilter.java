package store.common.client;

import com.google.common.base.Joiner;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
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

    private static final String REQUEST_PREFIX = "> ";
    private static final String RESPONSE_PREFIX = "< ";
    private static final Comparator<Entry<String, List<String>>> COMPARATOR =
            new Comparator<Entry<String, List<String>>>() {
        @Override
        public int compare(Entry<String, List<String>> o1, Entry<String, List<String>> o2) {
            return StringIgnoreCaseKeyComparator.SINGLETON.compare(o1.getKey(), o2.getKey());
        }
    };
    private final LoggingHandler handler;

    /**
     * Constructor.
     *
     * @param handler Logging adapter.
     */
    ClientLoggingFilter(LoggingHandler handler) {
        this.handler = handler;
    }

    @Override
    public void filter(ClientRequestContext context) {
        StringBuilder builder = new StringBuilder();
        printRequestLine(builder, context.getMethod(), context.getUri());
        printPrefixedHeaders(builder, REQUEST_PREFIX, context.getStringHeaders());
        handler.log(builder.toString());
    }

    @Override
    public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) {
        StringBuilder builder = new StringBuilder();
        printResponseLine(builder, responseContext.getStatus());
        printPrefixedHeaders(builder, RESPONSE_PREFIX, responseContext.getHeaders());
        builder.append(System.lineSeparator());
        handler.log(builder.toString());
    }

    private void printRequestLine(StringBuilder builder, String method, URI uri) {
        builder.append(REQUEST_PREFIX)
                .append(method)
                .append(" ")
                .append(uri.toASCIIString())
                .append(System.lineSeparator());
    }

    private void printResponseLine(StringBuilder builder, int status) {
        builder.append(RESPONSE_PREFIX)
                .append(Integer.toString(status))
                .append(System.lineSeparator());
    }

    private void printPrefixedHeaders(StringBuilder builder, String prefix, MultivaluedMap<String, String> headers) {
        TreeSet<Entry<String, List<String>>> sortedHeaders = new TreeSet<>(COMPARATOR);
        sortedHeaders.addAll(headers.entrySet());

        for (Entry<String, List<String>> headerEntry : sortedHeaders) {
            String header = headerEntry.getKey();
            String value;
            if (headerEntry.getValue().size() == 1) {
                value = headerEntry.getValue().get(0).toString();
            } else {
                value = Joiner.on(',').join(headerEntry.getValue());
            }
            builder.append(prefix)
                    .append(header)
                    .append(": ")
                    .append(value)
                    .append(System.lineSeparator());
        }
    }
}
