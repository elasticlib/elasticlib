package store.client.command;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;
import static store.client.command.AbstractCommand.REPOSITORY;

class Set extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap(REPOSITORY, asList(Type.REPOSITORY));

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
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
