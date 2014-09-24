package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.resolveRepositoryGuid;
import store.common.hash.Guid;

class DropRepository extends AbstractCommand {

    DropRepository() {
        super(Category.REPOSITORIES, Type.REPOSITORY);
    }

    @Override
    public String description() {
        return "Physically delete an existing repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Guid guid = resolveRepositoryGuid(session.getClient(), params.get(0));
        session.getClient()
                .repositories()
                .delete(guid);

        session.leave(guid);
        display.printOk();
    }
}
