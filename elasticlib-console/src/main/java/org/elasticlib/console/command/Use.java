package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Use extends AbstractCommand {

    Use() {
        super(Category.NODE, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Select repository to use";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.use(params.get(0));
        display.printOk();
    }
}
