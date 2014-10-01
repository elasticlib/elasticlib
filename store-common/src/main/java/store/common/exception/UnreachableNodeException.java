package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown when trying to connect to a remote node if none of its publish hosts responds.
 */
public final class UnreachableNodeException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return SERVICE_UNAVAILABLE;
    }

    @Override
    public String getMessage() {
        return "Remote node is not reachable";
    }
}
