package store.server.exception;

public final class VolumeAlreadyExistsException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "A volume with this name already exists";
    }
}
