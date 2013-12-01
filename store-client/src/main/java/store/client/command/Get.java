package store.client.command;

import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Session;
import store.client.Type;
import static store.client.Type.HASH;
import static store.client.Type.VOLUME;
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
    public void execute(Session session, List<String> args) {
        String volume = session.getVolume();
        Hash hash = new Hash(args.get(1));
        session.getRestClient().get(volume, hash);
    }
}
