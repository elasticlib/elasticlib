package store.common.config;

/**
 * Thrown if failure happens when reading or writing config.
 */
public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param e Root cause.
     */
    public ConfigException(Exception e) {
        super(e);
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public ConfigException(String message) {
        super(message);
    }
}
