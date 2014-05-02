package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.file.Directories;
import store.client.http.Session;

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
        display.println(Directories.workingDirectory().toString());
    }
}
