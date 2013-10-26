package store.server.exception;

public final class NoVolumeException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "There is no selected volume";
    }
}
