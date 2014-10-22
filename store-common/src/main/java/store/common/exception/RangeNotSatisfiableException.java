package store.common.exception;

import static javax.ws.rs.core.Response.Status.REQUESTED_RANGE_NOT_SATISFIABLE;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown when a HTTP range request fails because requested range is not satisfiable.
 */
public final class RangeNotSatisfiableException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return REQUESTED_RANGE_NOT_SATISFIABLE;
    }

    @Override
    public String getMessage() {
        return "Requested range is not satisfiable";
    }
}
