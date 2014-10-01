package store.common.exception;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static store.common.exception.ExceptionUtil.message;

/**
 * Thrown if an unexpected exception occurs on a node.
 */
public final class NodeFailureException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public NodeFailureException() {
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public NodeFailureException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception to wrap.
     */
    public NodeFailureException(Throwable cause) {
        super(message(cause), cause);
    }

    @Override
    public int getCode() {
        return code(INTERNAL_SERVER_ERROR, 01);
    }
}
