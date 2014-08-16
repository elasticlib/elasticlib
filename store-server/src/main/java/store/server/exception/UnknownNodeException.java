package store.server.exception;

/**
 * Thrown if an operation references an unknown node.
 */
public final class UnknownNodeException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This node is unknown";
    }
}
