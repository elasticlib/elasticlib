package store.server.exception;

public final class IndexAlreadyExistsException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "An index with this name already exists";
    }
}
