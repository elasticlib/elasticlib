package store.client.command;

import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import static store.client.command.Type.HASH;
import static store.client.command.Type.VOLUME;
import store.common.Hash;

class Get extends AbstractCommand {

    @Override
    public List<Type> env() {
        return singletonList(VOLUME);
    }

    @Override
    public List<Type> args() {
        return singletonList(HASH);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        Hash hash = new Hash(args.get(1));
        session.getRestClient().get(volume, hash);
    }
}
