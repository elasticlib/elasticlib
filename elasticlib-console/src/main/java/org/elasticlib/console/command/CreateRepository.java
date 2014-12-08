package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import org.elasticlib.console.util.Directories;

class CreateRepository extends AbstractCommand {

    CreateRepository() {
        super(Category.REPOSITORIES, Type.PATH);
    }

    @Override
    public String description() {
        return "Create a new repository";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .repositories()
                .create(Directories.resolve(params.get(0)));

        display.printOk();
    }
}
