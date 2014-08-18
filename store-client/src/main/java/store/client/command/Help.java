package store.client.command;

import java.util.List;
import static store.client.command.CommandProvider.commands;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Help extends AbstractCommand {

    Help() {
        super(Category.MISC, Type.COMMAND);
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
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        if (params.size() != 1 || commands(first(params)).isEmpty()) {
            display.println(CommandProvider.help());
            return;
        }
        for (Command command : commands(first(params))) {
            display.println(new StringBuilder()
                    .append(command.description())
                    .append(System.lineSeparator())
                    .append(command.usage())
                    .append(System.lineSeparator())
                    .toString());
        }
    }

    private static String first(List<String> params) {
        return params.get(0).toLowerCase();
    }
}
