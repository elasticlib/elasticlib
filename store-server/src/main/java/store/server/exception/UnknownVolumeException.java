package store.server.exception;

public final class UnknownVolumeException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This volume is unknown";
    }
}
