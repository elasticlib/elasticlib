package store.client.command;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import store.client.Display;
import store.client.Session;

class Put extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        return completeImpl(session, args, Type.PATH);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        Path path = Paths.get(args.get(1));
        session.getRestClient().put(volume, path);
    }
}
