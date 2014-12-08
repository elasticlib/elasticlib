package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

/**
 * A command.
 */
interface Command {

    /**
     * @return This command name.
     */
    String name();

    /**
     * @return This command category.
     */
    Category category();

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
     * Extract parameters list from supplied command line argument list (ie truncate command name).
     *
     * @param argList Command line argument list.
     * @return Corresponding parameters list.
     */
    List<String> params(List<String> argList);

    /**
     * Complete the supplied parameters list.
     *
     * @param completer Parameters completer.
     * @param params Parameters (Exclude command name).
     * @return A list of candidates for completion.
     */
    List<String> complete(ParametersCompleter completer, List<String> params);

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
    void execute(Display display, Session session, ConsoleConfig config, List<String> params);
}
