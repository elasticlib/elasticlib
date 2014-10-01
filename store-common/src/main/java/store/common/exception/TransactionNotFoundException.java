package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;

/**
 * Thrown when resuming a transaction if it does not exists or has expired.
 */
public final class TransactionNotFoundException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(SERVICE_UNAVAILABLE, 02);
    }

    @Override
    public String getMessage() {
        return "Transaction not found. It may have expired.";
    }
}
