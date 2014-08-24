package store.client.command;

import java.nio.file.Paths;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.client.util.Directories;

class CreateRepository extends AbstractCommand {

    CreateRepository() {
        super(Category.REPOSITORIES, Type.PATH);
    }

    @Override
    public String description() {
        return "Create a new repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().createRepository(Directories.resolve(Paths.get(params.get(0))));
        display.printOk();
    }
}
