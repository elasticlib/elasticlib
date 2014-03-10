package store.server.exception;

/**
 * Thrown at creation or opening of a repository if supplied path is invalid.
 */
public final class InvalidRepositoryPathException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Not a valid repository path";
    }
}
