package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Close extends AbstractCommand {

    Close() {
        super(Category.REPOSITORIES, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Close an existing repository";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .repositories()
                .close(params.get(0));

        display.printOk();
    }
}
