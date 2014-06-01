package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Close extends AbstractCommand {

    Close() {
        super(Category.SERVER, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Close an existing repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().closeRepository(params.get(0));
        display.println(OK);
    }
}
