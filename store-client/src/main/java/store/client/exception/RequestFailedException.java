package store.client.exception;

import java.io.IOException;

public class RequestFailedException extends RuntimeException {

    public RequestFailedException(String message) {
        super(message);
    }

    public RequestFailedException(IOException e) {
        super(e.getMessage());
    }
}
