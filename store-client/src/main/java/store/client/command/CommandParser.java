package store.client.command;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import static com.google.common.collect.Lists.newArrayList;
import java.util.Collections;
import static java.util.Collections.emptyList;
import java.util.Iterator;
import java.util.List;
import jline.console.completer.Completer;
import store.client.Session;
import store.client.Type;
import static store.client.Type.VOLUME;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private static final Command[] COMMANDS = new Command[]{new CreateVolume(),
                                                            new DropVolume(),
                                                            new Volumes(),
                                                            new CreateIndex(),
                                                            new DropIndex(),
                                                            new Indexes(),
                                                            new CreateReplication(),
                                                            new DropReplication(),
                                                            new Start(),
                                                            new Stop(),
                                                            new Put(),
                                                            new Delete(),
                                                            new Get(),
                                                            new Info(),
                                                            new Find(),
                                                            new History(),
                                                            new SetVolume(),
                                                            new UnsetVolume(),
                                                            new SetIndex(),
                                                            new UnsetIndex()};
    private final Session session;

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
            if (startWith(command, argList)) {
                return Optional.of(command);
            }
        }
        return Optional.absent();
    }

    private static boolean startWith(Command command, List<String> argList) {
        Iterator<String> it = argList.iterator();
        for (String part : Splitter.on("_").split(command.name())) {
            if (!it.hasNext() || !it.next().equalsIgnoreCase(part)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        if (buffer.length() != cursor) {
            return 0;
        }
        List<String> argList = Lists.newArrayList(Splitter.on(" ").trimResults().split(buffer));
        String argLine = Joiner.on("_").join(Lists.transform(argList, new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input.toUpperCase();
            }
        }));
        for (Command command : COMMANDS) {
            String commandLabel = command.name().toLowerCase().replaceAll("_", " ");
            if (!argLine.equals(command.name()) && command.name().startsWith(argLine)) {
                candidates.add(commandLabel);
            }
            if (!argLine.equals(command.name()) && argLine.startsWith(command.name())) {
                List<Type> types = command.args();
                List<String> values = args(command, argList);
                if (values.size() <= types.size()) {
                    int index = values.size() - 1;
                    List<String> completers = completers(types.get(index), values.get(index));
                    Collections.sort(completers);
                    values.add(0, commandLabel);
                    for (String completer : completers) {
                        values.set(index + 1, completer);
                        candidates.add(Joiner.on(" ").join(values));
                    }
                }
            }
            if (argLine.equals(command.name()) && !command.args().isEmpty()) {
                List<String> completers = completers(command.args().get(0), "");
                for (String completer : completers) {
                    candidates.add(Joiner.on(" ").join(commandLabel, completer));
                }
            }
        }
        return 0;
    }

    private static List<String> args(Command command, List<String> argList) {
        int parts = 0;
        Iterator<String> it = Splitter.on("_").split(command.name()).iterator();
        while (it.hasNext()) {
            it.next();
            parts++;
        }
        return argList.subList(parts, argList.size());
    }

    private List<String> completers(Type type, String value) {
        switch (type) {
            case VOLUME:
                return filterStartWith(session.getRestClient().listVolumes(), value);

            case INDEX:
                return filterStartWith(session.getRestClient().listIndexes(), value);

            default:
                return emptyList();
        }
    }

    private List<String> filterStartWith(List<String> list, final String item) {
        return newArrayList(Iterables.filter(list, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.startsWith(item);
            }
        }));
    }
}
