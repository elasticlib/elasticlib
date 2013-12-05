package store.client.command;

import java.nio.file.Path;
import java.nio.file.Paths;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import store.client.Type;
import static store.client.Type.PATH;
import static store.client.Type.VOLUME;

class Put extends AbstractCommand {

    @Override
    public List<Type> env() {
        return singletonList(VOLUME);
    }

    @Override
    public List<Type> args() {
        return singletonList(PATH);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        Path path = Paths.get(args.get(1));
        session.getRestClient().put(volume, path);
    }
}
