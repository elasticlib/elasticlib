package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Leave extends AbstractCommand {

    Leave() {
        super(Category.NODE);
    }

    @Override
    public String description() {
        return "Stop using current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.leave();
        display.printOk();
    }
}
