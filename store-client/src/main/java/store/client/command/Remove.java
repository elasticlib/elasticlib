package store.client.command;

import static java.util.Arrays.asList;
import java.util.List;
import static store.client.command.AbstractCommand.OK;
import static store.client.command.AbstractCommand.REPOSITORY;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Remove extends AbstractCommand {

    Remove() {
        super(Category.SERVER, REPOSITORY, asList(Type.REPOSITORY));
    }

    @Override
    public String description() {
        return "Remove an existing repository from this server, without deleting it";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().removeRepository(params.get(1));
        session.leave(params.get(1));
        display.println(OK);
    }
}
