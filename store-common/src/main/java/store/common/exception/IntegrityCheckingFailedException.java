package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown if a content integrity check fails.
 */
public final class IntegrityCheckingFailedException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 02);
    }

    @Override
    public String getMessage() {
        return "Integrity checking failed";
    }
}
