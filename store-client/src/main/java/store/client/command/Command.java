package store.client.command;

import java.util.List;
import java.util.Map;
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
     * @return Description of this command.
     */
    String description();

    /**
     *
     * @return Human readable command syntax.
     */
    String usage();

    /**
     * @return This command syntax.
     */
    Map<String, List<Type>> syntax();

    /**
     * Complete the supplied parameters list.
     *
     * @param session Current Session.
     * @param params Parameters (Exclude command name).
     * @return A list of candidates for completion.
     */
    List<String> complete(Session session, List<String> params);

    /**
     * Validate the supplied parameters list.
     *
     * @param params Parameters (Exclude command name).
     * @return True if parameters list matches command syntax.
     */
    boolean isValid(List<String> params);

    /**
     * Execute the command.
     *
     * @param display Display to output to.
     * @param session Session to execute against.
     * @param params Parameters (Exclude command name).
     */
    void execute(Display display, Session session, List<String> params);
}
