package store.client.command;

import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Stop extends AbstractCommand {

    @Override
    public Map<String, List<Type>> syntax() {
        return emptyMap();
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        session.getRestClient().stop(args.get(1));
    }
}
