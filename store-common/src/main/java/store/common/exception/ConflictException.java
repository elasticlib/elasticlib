package store.common.exception;

import static javax.ws.rs.core.Response.Status.CONFLICT;

/**
 * Thrown if a conflict happens while updating some content info in a repository.
 */
public final class ConflictException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(CONFLICT, 01);
    }

    @Override
    public String getMessage() {
        return "Conflict. Please retry.";
    }
}
