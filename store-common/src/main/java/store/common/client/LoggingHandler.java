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
    void logRequest(String message);

    /**
     * Logs the supplied message.
     *
     * @param message A log message.
     */
    void logResponse(String message);
}
