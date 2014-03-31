package store.client.command;

import java.util.List;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.CommandResult;
import store.common.Hash;

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
        CommandResult result = session.getRestClient().delete(repository, hash);
        display.print(asString(result));
    }
}
