package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Reset extends AbstractCommand {

    Reset() {
        super(Category.CONFIG);
    }

    @Override
    public String description() {
        return "Reset config";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        config.reset();
        display.println(OK);
    }
}
