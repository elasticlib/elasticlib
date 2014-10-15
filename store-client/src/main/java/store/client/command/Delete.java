package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import static store.client.util.ClientUtil.revisions;
import store.common.hash.Hash;
import store.common.model.CommandResult;
import store.common.model.ContentInfo;
import store.common.model.ContentState;

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
        ContentInfo contentInfo = session.getRepository().getContentInfo(hash);
        check(contentInfo);

        CommandResult result = session.getRepository().deleteContent(hash, revisions(contentInfo.getHead()));
        display.print(result);
    }

    private static void check(ContentInfo contentInfo) {
        if (contentInfo.getHead().isEmpty()) {
            throw new RequestFailedException("This content is unknown");
        }
        if (contentInfo.getState() != ContentState.PRESENT) {
            throw new RequestFailedException("This content is already deleted");
        }
    }
}
