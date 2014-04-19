package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.ReplicationDef;

class Replications extends AbstractCommand {

    Replications() {
        super(Category.SERVER);
    }

    @Override
    public String description() {
        return "List existing replications";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        for (ReplicationDef def : session.getClient().listReplicationDefs()) {
            display.print(def);
        }
    }
}
