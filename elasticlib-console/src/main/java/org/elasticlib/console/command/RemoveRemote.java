package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class RemoveRemote extends AbstractCommand {

    RemoveRemote() {
        super(Category.REMOTES, Type.NODE);
    }

    @Override
    public String description() {
        return "Remove a remote node";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .remotes()
                .remove(params.get(0));

        display.printOk();
    }
}
