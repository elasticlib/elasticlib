package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.model.NodeDef;

class Node extends AbstractCommand {

    Node() {
        super(Category.NODE);
    }

    @Override
    public String description() {
        return "Display info about current node";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        NodeDef def = session.getClient()
                .node()
                .getDef();

        display.print(def);
    }
}
