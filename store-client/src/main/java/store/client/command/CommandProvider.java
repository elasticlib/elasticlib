package store.client.command;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

final class CommandProvider {

    private static final List<Command> COMMANDS = Arrays.<Command>asList(new Config(),
                                                                         new Set(),
                                                                         new Unset(),
                                                                         new Reset(),
                                                                         new Connect(),
                                                                         new Disconnect(),
                                                                         new Use(),
                                                                         new Leave(),
                                                                         new Create(),
                                                                         new Drop(),
                                                                         new Repositories(),
                                                                         new Replications(),
                                                                         new Put(),
                                                                         new Update(),
                                                                         new Delete(),
                                                                         new Get(),
                                                                         new Info(),
                                                                         new Find(),
                                                                         new History(),
                                                                         new Quit(),
                                                                         new Help(),
                                                                         new OsCommand());

    static {
        Collections.sort(COMMANDS, new Comparator<Command>() {
            @Override
            public int compare(Command c1, Command c2) {
                return c1.name().compareTo(c2.name());
            }
        });
    }

    private CommandProvider() {
    }

    public static Optional<Command> command(String name) {
        for (Command command : COMMANDS) {
            if (command.name().equals(name)) {
                return Optional.of(command);
            }
        }
        return Optional.absent();
    }

    public static List<Command> commands() {
        return COMMANDS;
    }

    public static String help() {
        List<String> categoryHelps = new ArrayList<>();
        for (Category category : Category.values()) {
            StringBuilder builder = new StringBuilder();
            builder.append(category).append(System.lineSeparator());
            for (Command command : COMMANDS) {
                if (command.category() == category) {
                    builder.append(tab(2))
                            .append(fixedSize(command.name(), 15))
                            .append(command.description())
                            .append(System.lineSeparator());
                }
            }
            categoryHelps.add(builder.toString());
        }
        return Joiner.on(System.lineSeparator()).join(categoryHelps);
    }

    private static String tab(int size) {
        StringBuilder builder = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            builder.append(' ');
        }
        return builder.toString();
    }

    private static String fixedSize(String value, int size) {
        StringBuilder builder = new StringBuilder(value);
        for (int i = 0; i < size - value.length(); i++) {
            builder.append(' ');
        }
        return builder.toString();
    }
}
