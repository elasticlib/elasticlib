package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Remotes extends AbstractCommand {

    Remotes() {
        super(Category.REMOTES);
    }

    @Override
    public String description() {
        return "List existing remote nodes";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .remotes()
                .listInfos()
                .forEach(display::print);
    }
}
