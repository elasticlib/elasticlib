package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown at creation or opening of a repository if supplied path is invalid.
 */
public final class InvalidRepositoryPathException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return PRECONDITION_FAILED;
    }

    @Override
    public String getMessage() {
        return "Not a valid repository path";
    }
}
