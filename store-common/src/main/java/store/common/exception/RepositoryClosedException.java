package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

/**
 * Thrown if an operation fails because repository is closed.
 */
public final class RepositoryClosedException extends NodeException {

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
    public int getCode() {
        return code(SERVICE_UNAVAILABLE, 01);
    }

    @Override
    public String getMessage() {
        return "Repository is closed";
    }
}
