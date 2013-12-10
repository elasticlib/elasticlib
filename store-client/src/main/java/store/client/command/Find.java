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
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Search contents in current index";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String volume = session.getVolume();
        String index = session.getIndex();
        String query = params.get(0);
        session.getRestClient().find(volume, index, query);
    }
}
