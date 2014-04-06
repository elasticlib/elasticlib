package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.display.Display;
import store.client.http.Session;

class Disconnect extends AbstractCommand {

    Disconnect() {
        super();
    }

    @Override
    public String description() {
        return "Disconnect from current server";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        session.disconnect();
        display.println(OK);
    }
}
