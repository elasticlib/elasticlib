package store.client;

import com.google.common.base.Joiner;
import java.text.DateFormat;
import java.util.Map;
import static store.client.ByteLengthFormatter.format;
import store.common.CommandResult;
import store.common.ContentInfo;
import store.common.Digest;
import store.common.Event;
import store.common.metadata.Properties;
import store.common.metadata.Property;
import store.common.value.Value;

/**
 * Formating utilities.
 */
public final class FormatUtil {

    private static final String COMMA = " : ";

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

        Map<String, Value> metadata = info.getMetadata();
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
                .append(toPascalCase(event.getOperation().toString()))
                .append(System.lineSeparator())
                .append("Date")
                .append(COMMA)
                .append(DateFormat.getDateTimeInstance().format(event.getTimestamp()))
                .append(System.lineSeparator())
                .append("Hash")
                .append(COMMA)
                .append(event.getHash())
                .append(System.lineSeparator())
                .append("Head")
                .append(COMMA)
                .append(Joiner.on(", ").join(event.getHead()))
                .append(System.lineSeparator())
                .toString();
    }

    private static String toPascalCase(String lowerCase) {
        return Character.toUpperCase(lowerCase.charAt(0)) + lowerCase.substring(1);
    }

    /**
     * Format supplied command result.
     *
     * @param result An command result
     * @return A human readable string
     */
    public static String asString(CommandResult result) {
        if (result.isNoOp()) {
            return "Not modified";
        }
        return toPascalCase(result.getOperation().toString()) + 'd';
    }
}
