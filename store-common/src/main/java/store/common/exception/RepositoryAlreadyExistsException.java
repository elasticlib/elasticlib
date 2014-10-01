package store.common.exception;

import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;

/**
 * Thrown when creating or adding an already existing repository.
 */
public final class RepositoryAlreadyExistsException extends NodeException {

    private static final long serialVersionUID = 1L;

    @Override
    public int getCode() {
        return code(PRECONDITION_FAILED, 01);
    }

    @Override
    public String getMessage() {
        return "This repository already exists";
    }
}
