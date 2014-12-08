package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.model.CommandResult;
import org.elasticlib.common.model.ContentInfo;
import org.elasticlib.common.model.ContentState;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.parseHash;
import static org.elasticlib.console.util.ClientUtil.revisions;

class Delete extends AbstractCommand {

    Delete() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Delete an existing content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
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
