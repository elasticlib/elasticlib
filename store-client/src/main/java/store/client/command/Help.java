package store.client.command;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;
import static store.client.command.CommandProvider.command;

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
    public boolean isValid(List<String> params) {
        return true;
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        if (params.size() != 1 || !command(first(params)).isPresent()) {
            display.print(CommandProvider.help());
            return;
        }
        Command command = command(first(params)).get();
        display.print(new StringBuilder()
                .append(command.description())
                .append(System.lineSeparator())
                .append(command.usage())
                .toString());
    }

    private static String first(List<String> params) {
        return params.get(0).toLowerCase();
    }
}
