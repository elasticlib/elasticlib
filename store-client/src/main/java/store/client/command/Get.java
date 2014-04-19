package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.hash.Hash;

class Get extends AbstractCommand {

    Get() {
        super(Category.REPOSITORY, Type.HASH);
    }

    @Override
    public String description() {
        return "Get an existing content from current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        Hash hash = new Hash(params.get(0));
        session.getClient().get(repository, hash);
    }
}
