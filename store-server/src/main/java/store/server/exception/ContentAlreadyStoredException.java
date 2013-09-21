package store.server.exception;

public final class ContentAlreadyStoredException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This content is already stored";
    }
}
