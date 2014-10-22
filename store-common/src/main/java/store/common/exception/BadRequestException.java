package store.common.exception;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import javax.ws.rs.core.Response.StatusType;

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

    /**
     * Constructor.
     *
     * @param message Detail message explaining the error.
     * @param cause Cause exception.
     */
    public BadRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public StatusType getStatus() {
        return BAD_REQUEST;
    }
}
