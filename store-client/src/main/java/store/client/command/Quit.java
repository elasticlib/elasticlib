package store.client.command;

import java.util.List;
import store.client.Display;
import store.client.Session;
import static store.client.command.AbstractCommand.completeImpl;

class Quit extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        return completeImpl(session, args);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        session.close();
        System.exit(0);
    }
}
