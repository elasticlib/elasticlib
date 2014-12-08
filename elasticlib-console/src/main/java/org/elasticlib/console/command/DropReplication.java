package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class DropReplication extends AbstractCommand {

    DropReplication() {
        super(Category.REPLICATIONS, Type.REPOSITORY, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Physically delete an existing replication";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        session.getClient()
                .replications()
                .delete(params.get(0), params.get(1));

        display.printOk();
    }
}
