package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown if a replication of from repository to itself is requested.
 */
public final class SelfReplicationException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 04);
    }

    @Override
    public String getMessage() {
        return "A repository can not replicate to itself";
    }
}
