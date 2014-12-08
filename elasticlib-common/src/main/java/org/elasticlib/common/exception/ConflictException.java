package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if a conflict happens while updating some content info in a repository.
 */
public final class ConflictException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return CONFLICT;
    }

    @Override
    public String getMessage() {
        return "Conflict. Please retry.";
    }
}
