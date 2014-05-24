package store.server.exception;

/**
 * Thrown if request validation fails for any reason.
 */
public final class BadRequestException extends ServerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public BadRequestException() {
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public BadRequestException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public BadRequestException(Throwable cause) {
        super(cause);
    }
}
