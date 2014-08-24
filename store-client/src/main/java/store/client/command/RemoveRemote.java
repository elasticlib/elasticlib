package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class RemoveRemote extends AbstractCommand {

    RemoveRemote() {
        super(Category.REMOTES, Type.NODE);
    }

    @Override
    public String description() {
        return "Remove a remote node";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().removeRemote(params.get(0));
        display.printOk();
    }
}
