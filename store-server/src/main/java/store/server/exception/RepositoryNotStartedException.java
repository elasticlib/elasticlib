package store.server.exception;

/**
 * Thrown if an operation fails because repository is not started.
 */
public final class RepositoryNotStartedException extends ServerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public RepositoryNotStartedException() {
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public RepositoryNotStartedException(Throwable cause) {
        initCause(cause);
    }

    @Override
    public String getMessage() {
        return "Repository is not started";
    }
}
