package store.client.command;

import java.util.List;
import store.client.display.Display;
import store.client.http.Session;
import store.common.Hash;

class Get extends AbstractCommand {

    Get() {
        super(Type.HASH);
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
