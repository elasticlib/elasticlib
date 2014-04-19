package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.QuitException;
import store.client.http.Session;

class Quit extends AbstractCommand {

    Quit() {
        super(Category.MISC);
    }

    @Override
    public String description() {
        return "Leave this console";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        throw new QuitException();
    }
}
