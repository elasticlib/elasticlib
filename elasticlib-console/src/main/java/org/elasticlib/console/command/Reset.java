package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Reset extends AbstractCommand {

    Reset() {
        super(Category.CONFIG);
    }

    @Override
    public String description() {
        return "Reset config";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        config.reset();
        display.printOk();
    }
}
