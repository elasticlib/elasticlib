package store.common.client;

/**
 * Generic logging adapter.
 */
public interface LoggingHandler {

    /**
     * Logs the supplied message.
     *
     * @param message A log message.
     */
    public void logRequest(String message);

    /**
     * Logs the supplied message.
     *
     * @param message A log message.
     */
    public void logResponse(String message);
}
