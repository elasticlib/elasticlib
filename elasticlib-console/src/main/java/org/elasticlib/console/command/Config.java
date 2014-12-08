package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Config extends AbstractCommand {

    Config() {
        super(Category.CONFIG);
    }

    @Override
    public String description() {
        return "Display current config";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        display.println(config.print());
    }
}
