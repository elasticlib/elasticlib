package store.client.command;

import com.google.common.base.Splitter;
import static com.google.common.collect.Lists.newArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import jline.console.completer.Completer;
import store.client.Display;
import store.client.RequestFailedException;
import store.client.Session;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private static final List<Command> COMMANDS = Arrays.<Command>asList(new Create(),
                                                                         new Drop(),
                                                                         new Volumes(),
                                                                         new Indexes(),
                                                                         new Start(),
                                                                         new Stop(),
                                                                         new Put(),
                                                                         new Delete(),
                                                                         new Get(),
                                                                         new Info(),
                                                                         new Find(),
                                                                         new History(),
                                                                         new Set(),
                                                                         new Unset(),
                                                                         new Quit());

    static {
        Collections.sort(COMMANDS, new Comparator<Command>() {
            @Override
            public int compare(Command c1, Command c2) {
                return c1.name().compareTo(c2.name());
            }
        });
    }
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
        for (Command command : COMMANDS) {
            if (argList.get(0).equalsIgnoreCase(command.name())) {
                try {
                    command.execute(display, session, params(argList));

                } catch (RequestFailedException e) {
                    display.print(e.getMessage());
                }
                return;
            }
        }
        display.print("Unsupported command !");  // TODO print help
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

        for (Command command : COMMANDS) {
            String firstArg = argList.get(0).toLowerCase();
            if (argList.size() == 1) {
                if (command.name().equals(firstArg)) {
                    candidates.add(" ");

                } else if (command.name().startsWith(firstArg)) {
                    candidates.add(command.name());
                }
            } else if (firstArg.equals(command.name())) {
                candidates.addAll(command.complete(session, params(argList)));
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
            i = buffer.indexOf(arg, i);
        }
        return i;
    }

    private static List<String> argList(String buffer) {
        return newArrayList(Splitter
                .on(" ")
                .trimResults()
                .omitEmptyStrings()
                .split(buffer));
    }

    private static List<String> params(List<String> argList) {
        return argList.subList(1, argList.size());
    }
}
