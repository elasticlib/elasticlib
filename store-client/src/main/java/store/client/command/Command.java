package store.client.command;

import java.util.List;
import store.client.Display;
import store.client.Session;

/**
 * A command.
 */
interface Command {

    /**
     * @return This command name.
     */
    String name();

    /**
     * Complete the supplied arg line.
     *
     * @param session Current Session.
     * @param args Arguments (Excluding command name)
     * @return A list of candidates for completion
     */
    List<String> complete(Session session, List<String> args);

    /**
     * Execute the command.
     *
     * @param display Display to output to.
     * @param session Session to execute against.
     * @param args Command-line argument list (including command name).
     */
    void execute(Display display, Session session, List<String> args);
}
