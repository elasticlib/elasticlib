package store.client.command;

import com.google.common.base.Splitter;
import java.util.ArrayList;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.ws.rs.ProcessingException;
import jline.console.completer.Completer;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import store.client.tokenizing.Tokenizing;
import static store.client.tokenizing.Tokenizing.argList;
import static store.client.tokenizing.Tokenizing.isComplete;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private final Display display;
    private final Session session;
    private final ClientConfig config;

    /**
     * Constructor.
     *
     * @param display Display to inject.
     * @param session Session to inject.
     * @param config Config to inject.
     */
    public CommandParser(Display display, Session session, ClientConfig config) {
        this.display = display;
        this.session = session;
        this.config = config;
    }

    /**
     * Parse and execute supplied command line buffer.
     *
     * @param buffer Command line buffer.
     */
    public void execute(String buffer) {
        List<String> argList = argList(buffer);
        if (argList.isEmpty()) {
            return;
        }
        String firstArg = firstArg(argList);
        List<Command> commands = CommandProvider.commands(firstArg);
        if (commands.isEmpty()) {
            display.println(CommandProvider.help());
            return;
        }
        List<String> params = params(argList);
        for (Command command : commands) {
            if (command.isValid(params)) {
                execute(command, params);
                return;
            }
        }
        // If there is no matching command, print all usages.
        for (Command command : commands) {
            display.println(command.usage() + System.lineSeparator());
        }
    }

    private void execute(Command command, List<String> params) {
        try {
            session.printHttpDialog(true);
            command.execute(display, session, config, params);

        } catch (RequestFailedException e) {
            display.print(e);

        } catch (ProcessingException e) {
            String message = Splitter.on(':').limit(2).trimResults().splitToList(e.getMessage()).get(1);
            display.println(message + System.lineSeparator());
            session.disconnect();

        } finally {
            session.printHttpDialog(false);
        }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        if (buffer.length() != cursor) {
            return 0;
        }
        List<String> argList = argList(buffer);
        boolean emptyArg = buffer.isEmpty() || (isComplete(buffer) && buffer.endsWith(" "));
        if (emptyArg) {
            argList = new ArrayList<>(argList);
            argList.add("");
        }
        List<String> completions = completions(argList);
        candidates.addAll(completions);
        return emptyArg ? buffer.length() : Tokenizing.lastArgumentPosition(buffer);
    }

    private List<String> completions(List<String> argList) {
        String firstArg = firstArg(argList);
        List<Command> commands = CommandProvider.commands(firstArg);
        if (!commands.isEmpty() && argList.size() > 1) {
            for (Command command : commands) {
                List<String> completions = command.complete(session, params(argList));
                if (!completions.isEmpty()) {
                    return completions;
                }
            }
            return emptyList();
        }
        Set<String> completions = new TreeSet<>();
        for (Command command : CommandProvider.commands()) {
            if (command.name().startsWith(firstArg)) {
                completions.add(Splitter.on(' ').split(command.name()).iterator().next());
            }
        }
        return new ArrayList<>(completions);
    }

    private static String firstArg(List<String> argList) {
        return argList.get(0).toLowerCase();
    }

    private static List<String> params(List<String> argList) {
        return argList.subList(1, argList.size());
    }
}
