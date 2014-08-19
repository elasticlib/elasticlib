package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class RemoveRepository extends AbstractCommand {

    RemoveRepository() {
        super(Category.SERVER, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Remove an existing repository from this server, without deleting it";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().removeRepository(params.get(0));
        session.leave(params.get(1));
        display.println(OK);
    }
}
