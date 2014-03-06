package store.server.exception;

public final class UnknownContentException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This content is unknown";
    }
}
