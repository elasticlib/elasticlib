package store.common.exception;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import com.google.common.base.Joiner;

/**
 * Exception utilities
 */
class ExceptionUtil {

    private static final String EXCEPTION = "Exception";

    private ExceptionUtil() {
    }

    /**
     * Extracts message from supplied exception, containing its formatted name. Always returns a non empty string.
     *
     * @param exception An exception
     * @return a adequate message for this exception.
     */
    public static String message(Throwable exception) {
        if (exception.getMessage() != null && !exception.getMessage().isEmpty()) {
            return Joiner.on(" - ").join(name(exception), exception.getMessage());
        }
        return name(exception);
    }

    private static String name(Throwable exception) {
        String name = exception.getClass().getSimpleName();
        if (!name.endsWith(EXCEPTION)) {
            return toHumanCase(name);
        }
        return toHumanCase(truncate(name, EXCEPTION));
    }

    private static String truncate(String value, String suffix) {
        if (!value.endsWith(suffix)) {
            return value;
        }
        return value.substring(0, value.length() - suffix.length());
    }

    private static String toHumanCase(String className) {
        String lowerSpaced = UPPER_CAMEL.to(LOWER_HYPHEN, className).replace('-', ' ');
        return Character.toUpperCase(lowerSpaced.charAt(0)) + lowerSpaced.substring(1);
    }
}
