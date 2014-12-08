package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Leave extends AbstractCommand {

    Leave() {
        super(Category.NODE);
    }

    @Override
    public String description() {
        return "Stop using current repository";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.leave();
        display.printOk();
    }
}
