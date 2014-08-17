package store.server.exception;

/**
 * Thrown when trying to connect to a remote node if none of its publish hosts responds.
 */
public final class UnreachableNodeException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Remote node is not reachable";
    }
}
