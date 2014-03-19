package store.server.transaction;

import store.server.CommandResult;

/**
 * Functionnal interface defining a command, that is a pure mutative operation.
 */
public interface Command {

    /**
     * Invoke this command.
     *
     * @return This command invocation result.
     */
    CommandResult apply();
}
