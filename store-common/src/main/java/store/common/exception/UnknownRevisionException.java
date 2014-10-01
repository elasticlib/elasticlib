package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown if an operation references one or several unknown info revision(s).
 */
public final class UnknownRevisionException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 05);
    }

    @Override
    public String getMessage() {
        return "Unknown revision(s)";
    }
}
