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
    public String description() {
        return "Put a new content in current volume";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String volume = session.getVolume();
        Path path = Paths.get(params.get(0));
        session.getRestClient().put(volume, path);
    }
}
