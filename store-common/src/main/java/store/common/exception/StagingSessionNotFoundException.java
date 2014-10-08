package store.common.exception;

import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import javax.ws.rs.core.Response.StatusType;

/**
 * Thrown when loading a content staging session if it does not exists or has expired.
 */
public final class StagingSessionNotFoundException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public StatusType getStatus() {
        return SERVICE_UNAVAILABLE;
    }

    @Override
    public String getMessage() {
        return "Staging session not found. It may have expired.";
    }
}
