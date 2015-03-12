package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown when opening an already open repository.
 */
public final class RepositoryAlreadyOpenException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public RepositoryAlreadyOpenException() {
    }

    @Override
    public StatusType getStatus() {
        return PRECONDITION_FAILED;
    }

    @Override
    public String getMessage() {
        return "Repository is already open";
    }
}
