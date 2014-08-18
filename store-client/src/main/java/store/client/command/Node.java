package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Node extends AbstractCommand {

    Node() {
        super(Category.CONNECTION);
    }

    @Override
    public String description() {
        return "Display info about current node";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        display.print(session.getClient().getNode());
    }
}
