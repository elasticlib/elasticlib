package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Unset extends AbstractCommand {

    Unset() {
        super(Category.CONFIG, Type.KEY);
    }

    @Override
    public String description() {
        return "Unset a config value";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        config.unset(params.get(0));
        display.printOk();
    }
}
