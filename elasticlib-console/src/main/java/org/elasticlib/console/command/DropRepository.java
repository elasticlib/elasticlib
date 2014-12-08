package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.resolveRepositoryGuid;

class DropRepository extends AbstractCommand {

    DropRepository() {
        super(Category.REPOSITORIES, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Physically delete an existing repository";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Guid guid = resolveRepositoryGuid(session.getClient(), params.get(0));
        session.getClient()
                .repositories()
                .delete(guid);

        session.leave(guid);
        display.printOk();
    }
}
