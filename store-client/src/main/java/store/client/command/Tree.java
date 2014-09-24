package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.common.hash.Hash;
import store.common.model.ContentInfoTree;

class Tree extends AbstractCommand {

    Tree() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Print info revisions tree of an existing content";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        ContentInfoTree tree = session.getRepository().getInfoTree(hash);
        display.printTree(tree);
    }
}
