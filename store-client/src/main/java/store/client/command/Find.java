package store.client.command;

import java.util.Arrays;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import static store.client.command.Type.INDEX;
import static store.client.command.Type.QUERY;
import static store.client.command.Type.VOLUME;

class Find extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Arrays.asList(VOLUME, INDEX);
    }

    @Override
    public List<Type> args() {
        return singletonList(QUERY);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        String index = session.getIndex();
        String query = args.get(1);
        session.getRestClient().find(volume, index, query);
    }
}
