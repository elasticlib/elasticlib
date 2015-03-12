package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown when closing an already closed repository.
 */
public class RepositoryAlreadyClosedException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public RepositoryAlreadyClosedException() {
    }

    @Override
    public StatusType getStatus() {
        return PRECONDITION_FAILED;
    }

    @Override
    public String getMessage() {
        return "Repository is already closed";
    }
}
