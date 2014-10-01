package store.common.exception;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static store.common.exception.ExceptionUtil.message;

/**
 * Thrown if a IO error occurs on a node.
 */
public class IOFailedException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public IOFailedException() {
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public IOFailedException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception to wrap.
     */
    public IOFailedException(Throwable cause) {
        super(message(cause), cause);
    }

    @Override
    public int getCode() {
        return code(INTERNAL_SERVER_ERROR, 02);
    }
}
