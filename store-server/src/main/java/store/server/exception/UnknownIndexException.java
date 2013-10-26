package store.server.exception;

public final class UnknownIndexException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This index is unknown";
    }
}
