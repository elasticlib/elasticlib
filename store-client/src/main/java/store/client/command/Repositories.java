package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.model.RepositoryInfo;

class Repositories extends AbstractCommand {

    Repositories() {
        super(Category.REPOSITORIES);
    }

    @Override
    public String description() {
        return "List existing repositories";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        for (RepositoryInfo info : session.getClient().repositories().listInfos()) {
            display.print(info);
        }
    }
}
