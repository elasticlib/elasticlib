package store.exception;

import java.io.IOException;

public final class WriteException extends StoreException {

    private static final long serialVersionUID = 1L;

    public WriteException(IOException cause) {
        initCause(cause);
    }
}
