package store.server.exception;

/**
 * Thrown if adding the local node to tracked ones is requested.
 */
public final class SelfTrackingException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Can not track myself";
    }
}
