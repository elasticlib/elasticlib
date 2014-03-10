package store.server.transaction;

/**
 * Functionnal interface defining a command, that is a pure mutative operation.
 */
public interface Command {

    /**
     * Invoke this command.
     */
    void apply();
}
