package store.client.command;

import java.nio.file.Path;
import java.nio.file.Paths;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Put extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.PATH));

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        Path path = Paths.get(args.get(1));
        session.getRestClient().put(volume, path);
    }
}
