package store.client.command;

import java.util.List;
import static store.client.command.AbstractCommand.OK;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseUri;

class AddRemote extends AbstractCommand {

    AddRemote() {
        super(Category.REMOTES, Type.URL);
    }

    @Override
    public String description() {
        return "Add a remote node to current node";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().addRemote(parseUri(params.get(0)));
        display.println(OK);
    }
}
