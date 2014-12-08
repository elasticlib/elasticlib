package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.QuitException;
import org.elasticlib.console.http.Session;

class Quit extends AbstractCommand {

    Quit() {
        super(Category.MISC);
    }

    @Override
    public String description() {
        return "Leave this console";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        throw new QuitException();
    }
}
