package store.client.command;

import java.util.List;
import store.client.Display;
import store.client.QuitException;
import store.client.Session;

class Quit extends AbstractCommand {

    @Override
    public String description() {
        return "Leave this console";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        throw new QuitException();
    }
}
