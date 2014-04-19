package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Config extends AbstractCommand {

    @Override
    public String description() {
        return "Display current config";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        display.println(config.print());
    }
}
