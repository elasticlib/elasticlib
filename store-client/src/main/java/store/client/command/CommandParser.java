package store.client.command;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Lists.newArrayList;
import java.util.List;
import javax.ws.rs.ProcessingException;
import jline.console.completer.Completer;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.http.Session;

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
     * Parse and execute supplied command-line buffer.
     *
     * @param buffer Command-line buffer.
     */
    public void execute(String buffer) {
        List<String> argList = argList(buffer);
        if (argList.isEmpty()) {
            return;
        }
        String firstArg = firstArg(argList);
        Optional<Command> commandOpt = CommandProvider.command(firstArg);
        if (!commandOpt.isPresent()) {
            display.println(CommandProvider.help());
            return;
        }
        Command command = commandOpt.get();
        List<String> params = params(argList);
        if (!command.isValid(params)) {
            display.println(command.usage() + System.lineSeparator());
            return;
        }
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
        if (buffer.isEmpty() || buffer.endsWith(" ")) {
            argList.add("");
        }
        fillCandidates(candidates, argList);
        return cursor(buffer, candidates, argList);
    }

    private void fillCandidates(List<CharSequence> candidates, List<String> argList) {
        String firstArg = firstArg(argList);
        Optional<Command> commandOpt = CommandProvider.command(firstArg);
        if (commandOpt.isPresent()) {
            if (argList.size() == 1) {
                candidates.add(" ");
            } else {
                candidates.addAll(commandOpt.get().complete(session, params(argList)));
            }
        } else {
            for (Command command : CommandProvider.commands()) {
                if (command.name().startsWith(firstArg)) {
                    candidates.add(command.name());
                }
            }
        }
    }

    private static int cursor(String buffer, List<CharSequence> candidates, List<String> argList) {
        if (candidates.isEmpty()) {
            return 0;
        }
        if (buffer.isEmpty() || buffer.endsWith(" ") || (candidates.size() == 1 && candidates.get(0).equals(" "))) {
            return buffer.length();
        }
        int i = 0;
        for (String arg : argList) {
            i = buffer.indexOf(arg, i) + arg.length();
        }
        return i - getLast(argList).length();
    }

    private static List<String> argList(String buffer) {
        String trimmed = buffer.trim();
        if (trimmed.startsWith("!")) {
            trimmed = "! " + trimmed.substring(1);
        }
        return newArrayList(Splitter
                .on(" ")
                .trimResults()
                .omitEmptyStrings()
                .split(trimmed));
    }

    private static String firstArg(List<String> argList) {
        return argList.get(0).toLowerCase();
    }

    private static List<String> params(List<String> argList) {
        return argList.subList(1, argList.size());
    }
}
