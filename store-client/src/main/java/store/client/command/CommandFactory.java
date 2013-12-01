package store.client.command;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.List;

/**
 * Provide actual command implementations.
 */
public final class CommandFactory {

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

    private CommandFactory() {
    }

    /**
     * Provide command matching the supplied command-line argument list.
     *
     * @param argList Command-line argument list.
     * @return Matching command if any.
     */
    public static Optional<Command> command(List<String> argList) {
        for (Command command : COMMANDS) {
            if (matches(command, argList)) {
                return Optional.of(command);
            }
        }
        return Optional.absent();
    }

    private static boolean matches(Command command, List<String> argList) {
        Iterator<String> it = argList.iterator();
        for (String part : Splitter.on("_").split(command.name())) {
            if (!it.hasNext() || !it.next().equalsIgnoreCase(part)) {
                return false;
            }
        }
        return true;
    }
}
