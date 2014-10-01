package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

/**
 * Thrown when trying to connect to a remote node if none of its publish hosts responds.
 */
public final class UnreachableNodeException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(SERVICE_UNAVAILABLE, 03);
    }

    @Override
    public String getMessage() {
        return "Remote node is not reachable";
    }
}
