package store.server.exception;

public final class BadRequestException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Bad request";
    }
}
