package store.client.command;

import java.util.Collections;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Session;
import store.client.Type;
import static store.client.Type.VOLUME;

class Stop extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Collections.emptyList();
    }

    @Override
    public List<Type> args() {
        return singletonList(VOLUME);
    }

    @Override
    public void execute(Session session, List<String> args) {
        session.getRestClient().stop(args.get(1));
    }
}
