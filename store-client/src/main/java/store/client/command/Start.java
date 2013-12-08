package store.client.command;

import java.util.Collections;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import static store.client.command.Type.VOLUME;

class Start extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Collections.emptyList();
    }

    @Override
    public List<Type> args() {
        return singletonList(VOLUME);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        session.getRestClient().start(args.get(1));
    }
}
