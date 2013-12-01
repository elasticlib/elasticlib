package store.client;

import java.util.Map;
import static store.client.ByteLengthFormatter.format;
import store.common.ContentInfo;
import store.common.Digest;
import store.common.Event;
import store.common.Properties;
import store.common.Property;

/**
 * Formating utilities.
 */
public final class FormatUtil {

    private static final String COMMA = " : ";
    private static final String INDENT = "    ";

    private FormatUtil() {
    }

    /**
     * Format supplied content info.
     *
     * @param info A content info
     * @return A human readable string
     */
    public static String asString(ContentInfo info) {
        StringBuilder builder = new StringBuilder();
        builder.append("Hash")
                .append(COMMA)
                .append(info.getHash())
                .append(System.lineSeparator())
                .append("Length")
                .append(COMMA)
                .append(format(info.getLength()));

        Map<String, Object> metadata = info.getMetadata();
        for (Property property : Properties.list()) {
            if (metadata.containsKey(property.key())) {
                builder.append(System.lineSeparator())
                        .append(property.label())
                        .append(COMMA)
                        .append(metadata.get(property.key()));
            }
        }
        builder.append(System.lineSeparator());
        return builder.toString();
    }

    /**
     * Format supplied digest.
     *
     * @param digest A digest
     * @return A human readable string
     */
    public static String asString(Digest digest) {
        return new StringBuilder()
                .append("Hash")
                .append(COMMA)
                .append(digest.getHash())
                .append(System.lineSeparator())
                .append("Size")
                .append(COMMA)
                .append(format(digest.getLength()))
                .toString();
    }

    /**
     * Format supplied event.
     *
     * @param event An event
     * @return A human readable string
     */
    public static String asString(Event event) {
        return new StringBuilder()
                .append(event.getOperation().toString())
                .append(System.lineSeparator())
                .append(INDENT)
                .append("Hash")
                .append(COMMA)
                .append(event.getHash())
                .append(System.lineSeparator())
                .append(INDENT)
                .append("Date")
                .append(COMMA)
                .append(event.getTimestamp())
                .append(System.lineSeparator())
                .toString();
    }
}
