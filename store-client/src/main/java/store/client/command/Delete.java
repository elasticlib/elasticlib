package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.isDeleted;
import static store.client.util.ClientUtil.parseHash;
import static store.client.util.ClientUtil.revisions;
import store.common.client.RequestFailedException;
import store.common.hash.Hash;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;

class Delete extends AbstractCommand {

    Delete() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Delete an existing content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        List<ContentInfo> head = session.getRepository().getInfoHead(hash);
        if (isDeleted(head)) {
            throw new RequestFailedException("This content is already deleted");
        }
        CommandResult result = session.getRepository().deleteContent(hash, revisions(head));
        display.print(result);
    }
}
