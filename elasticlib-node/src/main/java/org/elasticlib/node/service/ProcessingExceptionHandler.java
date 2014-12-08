package org.elasticlib.node.service;

import java.net.SocketException;
import java.net.URI;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;

/**
 * Utility handler used to properly log instances of ProcessingException, thrown by client HTTP requests.
 */
public class ProcessingExceptionHandler {

    private final Logger logger;

    /**
     * Constructor.
     *
     * @param logger Underlying logger.
     */
    public ProcessingExceptionHandler(Logger logger) {
        this.logger = logger;
    }

    /**
     * Logs the supplied exception.
     *
     * @param target URI of the requested target.
     * @param exception Exception which happened.
     */
    public void log(URI target, ProcessingException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof SocketException) {
            logger.warn("Failed to request {} - {}: {}",
                        target,
                        cause.getClass().getSimpleName(),
                        cause.getMessage());
        } else {
            logger.warn("Failed to request " + target, exception);
        }
    }
}
