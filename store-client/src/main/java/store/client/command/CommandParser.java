package store.client.command;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Lists.newArrayList;
import java.util.List;
import jline.console.completer.Completer;
import store.client.display.Display;
import store.client.http.Session;
import store.client.exception.RequestFailedException;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private final Display display;
    private final Session session;

    /**
     * Constructor.
     *
     * @param display Display to inject.
     * @param session Session to inject.
     */
    public CommandParser(Display display, Session session) {
        this.display = display;
        this.session = session;
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
            command.execute(display, session, params);

        } catch (RequestFailedException e) {
            display.println(e.getMessage() + System.lineSeparator());
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
