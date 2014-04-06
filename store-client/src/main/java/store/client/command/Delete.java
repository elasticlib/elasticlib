package store.client.command;

import java.util.List;
import store.client.display.Display;
import store.client.http.Session;
import store.common.CommandResult;
import store.common.hash.Hash;

class Delete extends AbstractCommand {

    Delete() {
        super(Type.HASH);
    }

    @Override
    public String description() {
        return "Delete an existing content from current repository";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        Hash hash = new Hash(params.get(0));
        CommandResult result = session.getClient().delete(repository, hash);
        display.print(result);
    }
}
