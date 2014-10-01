package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown at creation or opening of a repository if supplied path is invalid.
 */
public final class InvalidRepositoryPathException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 03);
    }

    @Override
    public String getMessage() {
        return "Not a valid repository path";
    }
}
