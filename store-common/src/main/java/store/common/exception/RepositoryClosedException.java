package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import javax.ws.rs.core.Response.StatusType;

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
    public StatusType getStatus() {
        return SERVICE_UNAVAILABLE;
    }

    @Override
    public String getMessage() {
        return "Repository is closed";
    }
}
