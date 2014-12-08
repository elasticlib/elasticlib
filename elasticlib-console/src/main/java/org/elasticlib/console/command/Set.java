package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Set extends AbstractCommand {

    Set() {
        super(Category.CONFIG, Type.KEY, Type.VALUE);
    }

    @Override
    public String description() {
        return "Set a config value";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        config.set(params.get(0), params.get(1));
        display.printOk();
    }
}
