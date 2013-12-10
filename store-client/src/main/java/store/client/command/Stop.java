package store.client.command;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Stop extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.HASH));

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Stop a volume";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        session.getRestClient().stop(params.get(0));
    }
}
