package store.server.exception;

public final class StoreAlreadyExists extends StoreException {

    private static final long serialVersionUID = 1L;

    @Override
    public String getMessage() {
        return "There is already a store";
    }
}
