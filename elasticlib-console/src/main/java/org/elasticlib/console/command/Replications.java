package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Replications extends AbstractCommand {

    Replications() {
        super(Category.REPLICATIONS);
    }

    @Override
    public String description() {
        return "List existing replications";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .replications()
                .listInfos()
                .forEach(display::print);
    }
}
