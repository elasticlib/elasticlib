package store.exception;

public final class IntegrityCheckingFailedException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Integrity checking failed";
    }
}
