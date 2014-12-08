package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Repositories extends AbstractCommand {

    Repositories() {
        super(Category.REPOSITORIES);
    }

    @Override
    public String description() {
        return "List existing repositories";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .repositories()
                .listInfos()
                .forEach(display::print);
    }
}
