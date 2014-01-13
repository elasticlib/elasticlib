package store.server.exception;

public final class SelfReplicationException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "A repository can not replicate to itself";
    }
}
