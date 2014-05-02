package store.client.command;

import java.nio.file.Paths;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.Directories.changeToUserHome;
import static store.client.util.Directories.changeWorkingDirectory;

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
        if (params.get(0).equals("~")) {
            changeToUserHome();
            return;
        }
        changeWorkingDirectory(Paths.get(params.get(0)));
    }
}
