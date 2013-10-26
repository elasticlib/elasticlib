package store.server.exception;

public final class NoIndexException extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "There is no selected index";
    }
}
