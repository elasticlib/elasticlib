package store.common.client;

import com.google.common.base.Joiner;
import static java.lang.System.lineSeparator;
import java.util.List;
import java.util.Map.Entry;
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
        headers.entrySet()
                .stream()
                .sorted((o1, o2) -> StringIgnoreCaseKeyComparator.SINGLETON.compare(o1.getKey(), o2.getKey()))
                .forEach(header -> {
                    builder.append(prefix)
                    .append(header.getKey())
                    .append(": ")
                    .append(value(header))
                    .append(lineSeparator());
                });
    }

    private static String value(Entry<String, List<String>> header) {
        if (header.getValue().size() == 1) {
            return header.getValue().get(0);
        }
        return Joiner.on(',').join(header.getValue());
    }
}
