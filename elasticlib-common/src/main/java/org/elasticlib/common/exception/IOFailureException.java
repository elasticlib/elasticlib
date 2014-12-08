package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if a IO error occurs on a node.
 */
public class IOFailureException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public IOFailureException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception to wrap.
     */
    public IOFailureException(Throwable cause) {
        super(message(cause), cause);
    }

    @Override
    public StatusType getStatus() {
        return INTERNAL_SERVER_ERROR;
    }
}
