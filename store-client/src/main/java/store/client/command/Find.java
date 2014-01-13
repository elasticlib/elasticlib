package store.client.command;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Find extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.QUERY));

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Search contents in current repository";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        String query = params.get(0);
        session.getRestClient().find(repository, query);
    }
}
