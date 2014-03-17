package store.server.exception;

/**
 * Thrown when resuming a transaction if it does not exists or has expired.
 */
public class TransactionNotFoundException extends ServerException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "Transaction not found. It may have expired.";
    }
}
