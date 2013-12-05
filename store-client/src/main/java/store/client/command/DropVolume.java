package store.client.command;

import java.util.Collections;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import store.client.Type;
import static store.client.Type.VOLUME;

class DropVolume extends AbstractCommand {

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
        session.getRestClient().dropVolume(args.get(2));
    }
}
