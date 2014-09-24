package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class DropReplication extends AbstractCommand {

    DropReplication() {
        super(Category.REPLICATIONS, Type.REPOSITORY, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Physically delete an existing replication";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient()
                .replications()
                .delete(params.get(0), params.get(1));

        display.printOk();
    }
}
