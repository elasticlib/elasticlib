package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown when adding an already tracked node.
 */
public final class NodeAlreadyTrackedException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 06);
    }

    @Override
    public String getMessage() {
        return "This node is already tracked";
    }
}
