package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.Directories.workingDirectory;

class Pwd extends AbstractCommand {

    Pwd() {
        super(Category.MISC);
    }

    @Override
    public String description() {
        return "Print current working directory";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        display.println(workingDirectory().toString());
    }
}
