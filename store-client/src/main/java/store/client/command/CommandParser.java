package store.client.command;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import static com.google.common.collect.Lists.newArrayList;
import java.util.List;
import jline.console.completer.Completer;
import store.client.Session;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private static final Command[] COMMANDS = new Command[]{new Create(),
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
                                                            new Unset()};
    private final Session session;

    /**
     * Constructor.
     *
     * @param session Session to inject.
     */
    public CommandParser(Session session) {
        this.session = session;
    }

    /**
     * Provide command matching the supplied command-line argument list.
     *
     * @param argList Command-line argument list.
     * @return Matching command if any.
     */
    public Optional<Command> command(List<String> argList) {
        for (Command command : COMMANDS) {
            if (!argList.isEmpty() && argList.get(0).equalsIgnoreCase(command.name())) {
                return Optional.of(command);
            }
        }
        return Optional.absent();
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        if (buffer.length() != cursor) {
            return 0;
        }
        List<String> argList = newArrayList(Splitter
                .on(" ")
                .trimResults()
                .omitEmptyStrings()
                .split(buffer));

        if (buffer.isEmpty() || buffer.endsWith(" ")) {
            argList.add("");
        }

        for (Command command : COMMANDS) {
            String firstArg = argList.get(0).toLowerCase();
            String commandLabel = command.name().toLowerCase();
            if (argList.size() == 1) {
                if (commandLabel.equals(firstArg)) {
                    candidates.add(" ");

                } else if (commandLabel.startsWith(firstArg)) {
                    candidates.add(commandLabel);
                }
            } else if (firstArg.equals(commandLabel)) {
                candidates.addAll(command.complete(session, argList.subList(1, argList.size())));
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
}
