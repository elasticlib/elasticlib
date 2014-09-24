package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.resolveRepositoryGuid;
import store.common.hash.Guid;

class RemoveRepository extends AbstractCommand {

    RemoveRepository() {
        super(Category.REPOSITORIES, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Remove an existing repository, without deleting it";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Guid guid = resolveRepositoryGuid(session.getClient(), params.get(0));
        session.getClient()
                .repositories()
                .remove(guid);

        session.leave(guid);
        display.printOk();
    }
}
