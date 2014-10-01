package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown when resuming a transaction if it does not exists or has expired.
 */
public final class TransactionNotFoundException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return SERVICE_UNAVAILABLE;
    }

    @Override
    public String getMessage() {
        return "Transaction not found. It may have expired.";
    }
}
