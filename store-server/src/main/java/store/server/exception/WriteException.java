package store.server.exception;

import java.io.IOException;

/**
 * Thrown if an unexpected I/O exception occurs.
 */
public final class WriteException extends ServerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param cause Cause I/O exception to wrap.
     */
    public WriteException(IOException cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "Write error occured : " + getCause().getMessage();
    }
}
