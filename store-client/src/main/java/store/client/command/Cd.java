package store.client.command;

import java.nio.file.Paths;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.file.Directories;
import store.client.http.Session;

class Cd extends AbstractCommand {

    Cd() {
        super(Category.MISC, Type.DIRECTORY);
    }

    @Override
    public String description() {
        return "Change current working directory";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        if (params.isEmpty() || params.get(0).equals("~")) {
            Directories.changeToUserHome();
            return;
        }
        Directories.changeWorkingDirectory(Paths.get(params.get(0)));
    }
}
