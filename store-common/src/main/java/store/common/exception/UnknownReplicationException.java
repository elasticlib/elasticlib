package store.common.exception;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

/**
 * Thrown if an operation references an unknown replication.
 */
public final class UnknownReplicationException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(NOT_FOUND, 02);
    }

    @Override
    public String getMessage() {
        return "This replication is unknown";
    }
}
