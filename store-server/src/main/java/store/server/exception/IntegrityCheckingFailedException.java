package store.server.exception;

/**
 * Thrown if a content integrity check fails.
 */
public final class IntegrityCheckingFailedException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Integrity checking failed";
    }
}
