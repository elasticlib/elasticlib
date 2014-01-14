package store.server.exception;

public final class BadRequestException extends StoreException {

    private static final long serialVersionUID = 1L;
    private final String message;

    public BadRequestException() {
        this("Bad request");
    }

    public BadRequestException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
