package store.server.storage;

/**
 * Functionnal interface defining a procedure, that is an operation which returns nothing.
 */
public interface Procedure {

    /**
     * Invoke this procedure.
     */
    void apply();
}
