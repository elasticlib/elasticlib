package store.client.command;

import static java.util.Arrays.asList;
import java.util.List;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.command.AbstractCommand.REPOSITORY;

class Set extends AbstractCommand {

    Set() {
        super(REPOSITORY, asList(Type.REPOSITORY));
    }

    @Override
    public String description() {
        return "Set a session variable";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = params.get(1);
        session.setRepository(repository);
        display.setPrompt(repository);
    }
}
