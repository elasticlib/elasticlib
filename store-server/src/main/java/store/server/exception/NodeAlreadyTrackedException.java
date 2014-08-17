package store.server.exception;

/**
 * Thrown when adding an already tracked node.
 */
public final class NodeAlreadyTrackedException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This node is already tracked";
    }
}
