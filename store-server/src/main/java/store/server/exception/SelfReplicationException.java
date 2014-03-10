package store.server.exception;

/**
 * Thrown if a replication of from repository to itself is requested.
 */
public final class SelfReplicationException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "A repository can not replicate to itself";
    }
}
