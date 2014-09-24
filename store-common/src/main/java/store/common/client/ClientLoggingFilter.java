package store.common.client;

import com.google.common.base.Joiner;
import static java.lang.System.lineSeparator;
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

    private static final String SPACE = " ";
    private static final String DASH = " - ";
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
        StringBuilder builder = new StringBuilder()
                .append(REQUEST_PREFIX)
                .append(context.getMethod())
                .append(SPACE)
                .append(context.getUri().toASCIIString())
                .append(lineSeparator());

        printPrefixedHeaders(builder, REQUEST_PREFIX, context.getStringHeaders());

        handler.logRequest(builder.toString());
    }

    @Override
    public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) {
        StringBuilder builder = new StringBuilder()
                .append(RESPONSE_PREFIX)
                .append(responseContext.getStatus())
                .append(DASH)
                .append(responseContext.getStatusInfo().getReasonPhrase())
                .append(lineSeparator());

        printPrefixedHeaders(builder, RESPONSE_PREFIX, responseContext.getHeaders());

        handler.logResponse(builder.toString());
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
                    .append(lineSeparator());
        }
    }
}
