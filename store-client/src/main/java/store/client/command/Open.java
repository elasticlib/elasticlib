package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Open extends AbstractCommand {

    Open() {
        super(Category.SERVER, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Open an existing repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().openRepository(params.get(0));
        display.println(OK);
    }
}
