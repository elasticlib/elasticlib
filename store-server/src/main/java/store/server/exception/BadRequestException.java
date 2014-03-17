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
     * @param cause Cause exception.
     */
    public BadRequestException(Throwable cause) {
        initCause(cause);
    }
}
