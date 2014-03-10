package store.server.exception;

/**
 * Thrown if a rev-spec checking fails.
 */
public final class RevSpecCheckingFailedException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "RevSpec checking failed";
    }
}
