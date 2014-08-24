package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Unset extends AbstractCommand {

    Unset() {
        super(Category.CONFIG, Type.KEY);
    }

    @Override
    public String description() {
        return "Unset a config value";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        config.unset(params.get(0));
        display.printOk();
    }
}
