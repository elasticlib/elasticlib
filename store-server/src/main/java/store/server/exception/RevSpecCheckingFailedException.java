package store.server.exception;

public final class RevSpecCheckingFailedException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "RevSpec checking failed";
    }
}
