package store.server.exception;

/**
 * Thrown if request validation fails for any reason.
 */
public final class BadRequestException extends ServerException {

    private static final long serialVersionUID = 1L;
    private final String message;

    /**
     * Constructor.
     */
    public BadRequestException() {
        this("Bad request");
    }

    /**
     * Constructor.
     *
     * @param message Detail message.
     */
    public BadRequestException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
