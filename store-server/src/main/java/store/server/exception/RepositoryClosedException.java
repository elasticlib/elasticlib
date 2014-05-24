package store.server.exception;

/**
 * Thrown if an operation fails because repository is closed.
 */
public final class RepositoryClosedException extends ServerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public RepositoryClosedException() {
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public RepositoryClosedException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Repository is closed";
    }
}
