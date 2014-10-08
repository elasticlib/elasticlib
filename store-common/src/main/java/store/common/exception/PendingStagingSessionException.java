package store.common.exception;

import static javax.ws.rs.core.Response.Status.CONFLICT;
import javax.ws.rs.core.Response.StatusType;

/**
 *
 * Thrown when starting a staging session for a given content if another one is already in progress for this content.
 */
public final class PendingStagingSessionException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return CONFLICT;
    }

    @Override
    public String getMessage() {
        return "There is already another staging session in progress for this content.";
    }
}
