package store.client.command;

import com.google.common.base.Optional;
import java.util.List;
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
        Optional<Command> commandOpt = CommandProvider.command(params);
        if (!commandOpt.isPresent()) {
            display.println(CommandProvider.help());
            return;
        }
        Command command = commandOpt.get();
        display.println(new StringBuilder()
                .append(command.description())
                .append(System.lineSeparator())
                .append(command.usage())
                .append(System.lineSeparator())
                .toString());

    }
}
