package store.client.command;

import java.util.Collections;
import java.util.List;
import store.client.Display;
import store.client.Session;
import static store.client.command.AbstractCommand.REPOSITORY;

class Unset extends AbstractCommand {

    Unset() {
        super(REPOSITORY, Collections.<Type>emptyList());
    }

    @Override
    public String description() {
        return "Unset a session variable";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        session.unsetRepository();
        display.resetPrompt();
    }
}
