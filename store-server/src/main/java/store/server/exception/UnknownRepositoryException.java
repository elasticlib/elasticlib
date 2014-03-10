package store.server.exception;

/**
 * Thrown if an operation references an unknown repository.
 */
public final class UnknownRepositoryException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This repository is unknown";
    }
}
