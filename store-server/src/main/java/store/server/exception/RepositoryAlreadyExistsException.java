package store.server.exception;

/**
 * Thrown when creating or adding an already existing repository.
 */
public final class RepositoryAlreadyExistsException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This repository already exists";
    }
}
