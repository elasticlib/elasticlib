package store.server.exception;

public final class RepositoryAlreadyExistsException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "A Repository with this name already exists";
    }
}
