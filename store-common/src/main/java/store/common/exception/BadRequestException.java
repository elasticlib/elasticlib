package store.common.exception;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static store.common.exception.ExceptionUtil.message;

/**
 * Thrown if request validation fails for any reason.
 */
public final class BadRequestException extends NodeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     */
    public BadRequestException() {
    }

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     */
    public BadRequestException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause Cause exception.
     */
    public BadRequestException(Throwable cause) {
        super(message(cause), cause);
    }

    @Override
    public int getCode() {
        return code(BAD_REQUEST, 01);
    }
}
