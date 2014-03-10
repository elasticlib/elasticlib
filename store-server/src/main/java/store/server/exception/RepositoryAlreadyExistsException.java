package store.server.exception;

/**
 * Thrown when creating a new repository if supplied name is already used.
 */
public final class RepositoryAlreadyExistsException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "A Repository with this name already exists";
    }
}
