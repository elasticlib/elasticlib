package store.client.command;

import static java.util.Arrays.asList;
import java.util.List;
import static store.client.command.AbstractCommand.OK;
import static store.client.command.AbstractCommand.REPLICATION;
import static store.client.command.AbstractCommand.REPOSITORY;
import store.client.display.Display;
import store.client.http.Session;

class Drop extends AbstractCommand {

    Drop() {
        super(REPOSITORY, asList(Type.REPOSITORY),
              REPLICATION, asList(Type.REPOSITORY, Type.REPOSITORY));
    }

    @Override
    public String description() {
        return "Drop an existing repository or replication";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        switch (params.get(0).toLowerCase()) {
            case REPOSITORY:
                session.getClient().dropRepository(params.get(1));
                session.stopUse(params.get(1));
                break;

            case REPLICATION:
                session.getClient().dropReplication(params.get(1), params.get(2));
                break;

            default:
                throw new IllegalArgumentException(params.get(0));
        }
        display.println(OK);
    }
}
