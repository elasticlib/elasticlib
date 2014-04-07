package store.client.exception;

import java.io.IOException;

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
    public RequestFailedException(IOException e) {
        super(e.getMessage());
    }
}
