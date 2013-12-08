package store.client.command;

import java.util.List;
import store.client.Display;
import store.client.Session;

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
     * @param display Display to output to.
     * @param session Session to execute against.
     * @param args Command-line argument list (including command name).
     */
    void execute(Display display, Session session, List<String> args);
}
