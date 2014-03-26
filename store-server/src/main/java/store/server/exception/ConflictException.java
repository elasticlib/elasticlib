package store.server.exception;

/**
 * Thrown if a conflict happens while updating a repository checking fails.
 */
public final class ConflictException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Conflict. Please retry.";
    }
}
