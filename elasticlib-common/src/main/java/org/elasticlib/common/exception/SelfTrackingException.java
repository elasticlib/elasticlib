package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if adding the local node to tracked ones is requested.
 */
public final class SelfTrackingException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return PRECONDITION_FAILED;
    }

    @Override
    public String getMessage() {
        return "Can not track myself";
    }
}
