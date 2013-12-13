package store.client.command;

import com.google.common.base.Optional;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Help extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.COMMAND));

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Print help about a command";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        Optional<Command> commandOpt = CommandProvider.command(params.get(0).toLowerCase());
        if (commandOpt.isPresent()) {
            display.print(help(commandOpt.get()));
        } else {
            display.print(CommandProvider.help());
        }
    }

    private static String help(Command command) {
        return new StringBuilder()
                .append(command.description())
                .append(System.lineSeparator())
                .append(command.usage())
                .toString();
    }
}
