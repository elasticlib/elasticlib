package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Use extends AbstractCommand {

    Use() {
        super(Category.NODE, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Select repository to use";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.use(params.get(0));
        display.printOk();
    }
}
