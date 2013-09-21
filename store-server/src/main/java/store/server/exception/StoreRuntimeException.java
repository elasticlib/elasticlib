package store.server.exception;

public class StoreRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StoreRuntimeException(Throwable cause) {
        super(cause);
    }
}
