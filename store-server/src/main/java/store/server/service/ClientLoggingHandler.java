package store.server.service;

import static java.lang.System.lineSeparator;
import org.slf4j.Logger;
import store.common.client.LoggingHandler;

/**
 * Utility handler used to route node client logging messages to SLF4J.
 */
public class ClientLoggingHandler implements LoggingHandler {

    private final Logger logger;

    /**
     * Constructor.
     *
     * @param logger Underlying logger.
     */
    public ClientLoggingHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void logRequest(String message) {
        logger.info("Sending request{}{}", lineSeparator(), message);
    }

    @Override
    public void logResponse(String message) {
        logger.info("Received response{}{}", lineSeparator(), message);
    }
}
