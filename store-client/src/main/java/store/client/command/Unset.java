package store.client.command;

import java.util.Collections;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;
import static store.client.command.AbstractCommand.REPOSITORY;

class Unset extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap(REPOSITORY, Collections.<Type>emptyList());

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
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
