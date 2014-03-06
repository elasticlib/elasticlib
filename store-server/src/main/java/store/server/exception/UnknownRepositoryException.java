package store.server.exception;

public final class UnknownRepositoryException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This repository is unknown";
    }
}
