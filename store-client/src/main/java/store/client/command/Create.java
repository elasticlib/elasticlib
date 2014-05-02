package store.client.command;

import java.nio.file.Paths;
import static java.util.Arrays.asList;
import java.util.List;
import static store.client.command.AbstractCommand.OK;
import static store.client.command.AbstractCommand.REPLICATION;
import static store.client.command.AbstractCommand.REPOSITORY;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.util.Directories;
import store.client.http.Session;

class Create extends AbstractCommand {

    Create() {
        super(Category.SERVER,
              REPOSITORY, asList(Type.PATH),
              REPLICATION, asList(Type.REPOSITORY, Type.REPOSITORY));
    }

    @Override
    public String description() {
        return "Create a new repository or replication";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        switch (params.get(0).toLowerCase()) {
            case REPOSITORY:
                session.getClient().createRepository(Directories.resolve(Paths.get(params.get(1))));
                break;

            case REPLICATION:
                session.getClient().createReplication(params.get(1), params.get(2));
                break;

            default:
                throw new IllegalArgumentException(params.get(0));
        }
        display.println(OK);
    }
}
