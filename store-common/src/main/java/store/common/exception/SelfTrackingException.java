package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown if adding the local node to tracked ones is requested.
 */
public final class SelfTrackingException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 07);
    }

    @Override
    public String getMessage() {
        return "Can not track myself";
    }
}
