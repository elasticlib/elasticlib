package store.server.exception;

public final class RepositoryNotStartedException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Volume is not started";
    }
}
