package store.client.command;

import java.util.List;
import store.client.Display;
import store.client.Session;

class Find extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        return completeImpl(session, args, Type.QUERY);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        String index = session.getIndex();
        String query = args.get(1);
        session.getRestClient().find(volume, index, query);
    }
}
