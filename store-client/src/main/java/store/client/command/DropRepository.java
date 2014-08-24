package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class DropRepository extends AbstractCommand {

    DropRepository() {
        super(Category.REPOSITORIES, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Physically delete an existing repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().deleteRepository(params.get(0));
        session.leave(params.get(0));
        display.printOk();
    }
}
