package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.NodeDef;

class Remotes extends AbstractCommand {

    Remotes() {
        super(Category.REMOTES);
    }

    @Override
    public String description() {
        return "List existing remote nodes";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        for (NodeDef def : session.getClient().listRemotes()) {
            display.print(def);
        }
    }
}
