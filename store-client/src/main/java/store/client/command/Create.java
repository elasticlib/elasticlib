package store.client.command;

import java.nio.file.Paths;
import static java.util.Arrays.asList;
import java.util.List;
import store.client.Display;
import store.client.Session;
import static store.client.command.AbstractCommand.REPLICATION;
import static store.client.command.AbstractCommand.REPOSITORY;

class Create extends AbstractCommand {

    Create() {
        super(REPOSITORY, asList(Type.PATH),
              REPLICATION, asList(Type.REPOSITORY, Type.REPOSITORY));
    }

    @Override
    public String description() {
        return "Create a new repository or replication";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        switch (params.get(0).toLowerCase()) {
            case REPOSITORY:
                session.getRestClient().createRepository(Paths.get(params.get(1)));
                break;

            case REPLICATION:
                session.getRestClient().createReplication(params.get(1), params.get(2));
                break;

            default:
                throw new IllegalArgumentException(params.get(0));
        }
    }
}
