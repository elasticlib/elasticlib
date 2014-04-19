package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;

class Connect extends AbstractCommand {

    Connect() {
        super(Type.URL);
    }

    @Override
    public String description() {
        return "Connect to a server";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String url = params.get(0);
        session.connect(url);
        display.println(OK);
    }
}
