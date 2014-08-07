package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Stop extends AbstractCommand {

    Stop() {
        super(Category.SERVER, Type.REPOSITORY, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Stop an existing replication, without deleting it";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().stopReplication(params.get(0), params.get(1));
        display.println(OK);
    }
}