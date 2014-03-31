package store.client.command;

import java.util.List;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.command.CommandProvider.command;

class Help extends AbstractCommand {

    Help() {
        super(Type.COMMAND);
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
            display.println(CommandProvider.help());
            return;
        }
        Command command = command(first(params)).get();
        display.println(new StringBuilder()
                .append(command.description())
                .append(System.lineSeparator())
                .append(command.usage())
                .toString());
    }

    private static String first(List<String> params) {
        return params.get(0).toLowerCase();
    }
}
