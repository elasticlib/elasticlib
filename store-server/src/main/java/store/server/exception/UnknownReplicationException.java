package store.server.exception;

/**
 * Thrown if an operation references an unknown replication.
 */
public class UnknownReplicationException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "This replication is unknown";
    }
}
