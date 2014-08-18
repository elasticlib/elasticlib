package store.server.exception;

/**
 * Base class for all expected server-side exception.
 */
public abstract class ServerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public ServerException() {
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public ServerException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public ServerException(Throwable cause) {
        super(cause);
    }
}
