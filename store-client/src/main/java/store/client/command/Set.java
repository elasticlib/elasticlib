package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

public class Set extends AbstractCommand {

    Set() {
        super(Type.KEY, Type.VALUE);
    }

    @Override
    public String description() {
        return "Set a config value";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        config.set(params.get(0), params.get(1));
        display.println(OK);
    }
}
