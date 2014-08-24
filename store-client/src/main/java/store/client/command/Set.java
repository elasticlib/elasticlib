package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Set extends AbstractCommand {

    Set() {
        super(Category.CONFIG, Type.KEY, Type.VALUE);
    }

    @Override
    public String description() {
        return "Set a config value";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        config.set(params.get(0), params.get(1));
        display.printOk();
    }
}
