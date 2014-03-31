package store.client.command;

import java.util.List;
import store.client.display.Display;
import store.client.http.Session;
import store.client.exception.QuitException;

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
