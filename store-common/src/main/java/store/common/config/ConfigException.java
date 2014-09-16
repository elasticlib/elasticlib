package store.common.config;

/**
 * Thrown if failure happens when reading or writing config.
 */
public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param cause Root cause.
     */
    public ConfigException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public ConfigException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     * @param cause Root cause.
     */
    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
