package store.client.command;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;
import store.common.Hash;

class Delete extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.HASH));

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        Hash hash = new Hash(args.get(1));
        session.getRestClient().delete(volume, hash);
    }
}
