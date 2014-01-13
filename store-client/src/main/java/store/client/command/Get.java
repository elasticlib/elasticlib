package store.client.command;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;
import store.common.Hash;

class Get extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.HASH));

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Get an existing content from current repository";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        Hash hash = new Hash(params.get(0));
        session.getRestClient().get(repository, hash);
    }
}
