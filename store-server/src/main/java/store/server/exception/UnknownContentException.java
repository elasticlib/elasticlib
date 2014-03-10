package store.server.exception;

/**
 * Thrown if an operation references an unknown content.
 */
public final class UnknownContentException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This content is unknown";
    }
}
