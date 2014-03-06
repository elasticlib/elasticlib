package store.server.exception;

public final class UnknownRevisionException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Unknown revision(s)";
    }
}
