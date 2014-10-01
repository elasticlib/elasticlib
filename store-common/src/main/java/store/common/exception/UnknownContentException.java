package store.common.exception;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

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
    public int getCode() {
        return code(NOT_FOUND, 03);
    }

    @Override
    public String getMessage() {
        return "This content is unknown";
    }
}
