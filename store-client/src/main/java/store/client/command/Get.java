package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.common.hash.Hash;

class Get extends AbstractCommand {

    Get() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Get an existing content from current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        Hash hash = parseHash(params.get(0));
        session.getClient().get(repository, hash);
    }
}
