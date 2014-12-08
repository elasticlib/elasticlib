package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.resolveRepositoryGuid;

class RemoveRepository extends AbstractCommand {

    RemoveRepository() {
        super(Category.REPOSITORIES, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Remove an existing repository, without deleting it";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Guid guid = resolveRepositoryGuid(session.getClient(), params.get(0));
        session.getClient()
                .repositories()
                .remove(guid);

        session.leave(guid);
        display.printOk();
    }
}
