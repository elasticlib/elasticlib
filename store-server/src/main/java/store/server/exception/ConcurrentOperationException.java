package store.server.exception;

public final class ConcurrentOperationException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Another operation on this content is already in progress";
    }
}
