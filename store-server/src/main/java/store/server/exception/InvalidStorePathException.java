package store.server.exception;

public final class InvalidStorePathException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Not a valid store path";
    }
}
