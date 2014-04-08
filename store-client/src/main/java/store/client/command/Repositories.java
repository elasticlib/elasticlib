package store.client.command;

import java.util.List;
import store.client.display.Display;
import store.client.http.Session;
import store.common.RepositoryDef;

class Repositories extends AbstractCommand {

    @Override
    public String description() {
        return "List existing repositories";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        for (RepositoryDef def : session.getClient().listRepositoryDefs()) {
            display.print(def);
        }
    }
}
