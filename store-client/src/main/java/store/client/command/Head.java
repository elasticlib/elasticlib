package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.common.ContentInfo;
import store.common.hash.Hash;

class Head extends AbstractCommand {

    Head() {
        super(Category.REPOSITORY, Type.HASH);
    }

    @Override
    public String description() {
        return "Print info head revisions of an existing content in current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        Hash hash = parseHash(params.get(0));
        for (ContentInfo info : session.getClient().getInfoHead(repository, hash)) {
            display.print(info);
        }
    }
}
