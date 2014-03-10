package store.server.exception;

/**
 * Thrown if an operation references one or several unknown info revision(s).
 */
public final class UnknownRevisionException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Unknown revision(s)";
    }
}
