package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

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
        session.getClient()
                .repositories()
                .listInfos()
                .forEach(display::print);
    }
}
