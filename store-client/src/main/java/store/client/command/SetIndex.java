package store.client.command;

import java.util.Collections;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import store.client.Type;
import static store.client.Type.INDEX;

class SetIndex extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Collections.emptyList();
    }

    @Override
    public List<Type> args() {
        return singletonList(INDEX);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        session.setIndex(args.get(2));
    }
}
