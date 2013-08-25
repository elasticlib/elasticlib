package store.exception;

public final class UnknownHashException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This hash is unknown";
    }
}
