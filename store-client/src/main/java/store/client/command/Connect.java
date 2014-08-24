package store.client.command;

import java.net.URI;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseUri;

class Connect extends AbstractCommand {

    Connect() {
        super(Category.NODE, Type.URI);
    }

    @Override
    public String description() {
        return "Connect to a node";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        URI uri = parseUri(params.get(0));
        session.connect(uri);
        display.printOk();
    }
}
