package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Disconnect extends AbstractCommand {

    Disconnect() {
        super(Category.NODE);
    }

    @Override
    public String description() {
        return "Disconnect from current node";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.disconnect();
        display.printOk();
    }
}
