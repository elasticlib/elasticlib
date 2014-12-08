package org.elasticlib.console.exception;

/**
 * Thrown if a requests fails for any reason.
 */
public class RequestFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param message Error message.
     */
    public RequestFailedException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param e The cause of this exception.
     */
    public RequestFailedException(Exception e) {
        super(e.getMessage());
    }

    /**
     * Constructor.
     *
     * @param message Error message.
     * @param e The cause of this exception.
     */
    public RequestFailedException(String message, Exception e) {
        super(message + System.lineSeparator() + e.getMessage());
    }
}
