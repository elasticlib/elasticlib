package store.server.exception;

/**
 * Thrown if an operation references an unknown content.
 */
public final class UnknownContentException extends ServerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public UnknownContentException() {
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public UnknownContentException(Throwable cause) {
        initCause(cause);
    }

    @Override
    public String getMessage() {
        return "This content is unknown";
    }
}
