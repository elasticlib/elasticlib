package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if an operation references an unknown node.
 */
public final class UnknownNodeException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return NOT_FOUND;
    }

    @Override
    public String getMessage() {
        return "This node is unknown";
    }
}
