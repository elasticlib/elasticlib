package store.client.command;

import java.util.List;
import store.client.Session;
import store.client.Type;

/**
 * A command.
 */
public interface Command {

    /**
     * @return This command name.
     */
    String name();

    /**
     * @return Expected environment for this command.
     */
    List<Type> env();

    /**
     * @return Expected arguments for this command.
     */
    List<Type> args();

    /**
     * Execute the command.
     *
     * @param session Session to execute against.
     * @param args Command-line argument list (including command name).
     */
    void execute(Session session, List<String> args);
}
