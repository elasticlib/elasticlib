package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.common.CommandResult;
import store.common.hash.Hash;

class Delete extends AbstractCommand {

    Delete() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Delete an existing content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        Hash hash = parseHash(params.get(0));
        CommandResult result = session.getClient().delete(repository, hash);
        display.print(result);
    }
}
