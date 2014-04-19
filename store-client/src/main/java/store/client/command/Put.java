package store.client.command;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.CommandResult;

class Put extends AbstractCommand {

    Put() {
        super(Category.REPOSITORY, Type.PATH);
    }

    @Override
    public String description() {
        return "Put a new content in current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        Path path = Paths.get(params.get(0));
        CommandResult result = session.getClient().put(repository, path);
        display.print(result);
    }
}
