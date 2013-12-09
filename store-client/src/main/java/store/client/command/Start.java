package store.client.command;

import java.util.List;
import store.client.Display;
import store.client.Session;

class Start extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        return completeImpl(session, args, Type.VOLUME);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        session.getRestClient().start(args.get(1));
    }
}
