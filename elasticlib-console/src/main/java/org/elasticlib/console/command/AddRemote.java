package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.parseUri;

class AddRemote extends AbstractCommand {

    AddRemote() {
        super(Category.REMOTES, Type.URI);
    }

    @Override
    public String description() {
        return "Add a remote node to current node";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .remotes()
                .add(parseUri(params.get(0)));

        display.printOk();
    }
}
