package org.elasticlib.common.exception;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown if an operation references an unknown content.
 */
public final class UnknownContentException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public UnknownContentException() {
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public UnknownContentException(Throwable cause) {
        super(cause);
    }

    @Override
    public StatusType getStatus() {
        return NOT_FOUND;
    }

    @Override
    public String getMessage() {
        return "This content is unknown";
    }
}
