package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import static store.client.util.ClientUtil.parseHash;
import store.common.ContentInfoTree;
import store.common.hash.Hash;

class Tree extends AbstractCommand {

    Tree() {
        super(Category.REPOSITORY, Type.HASH);
    }

    @Override
    public String description() {
        return "Print info revisions tree of an existing content in current repository";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        Hash hash = parseHash(params.get(0));
        ContentInfoTree tree = session.getClient().getInfoTree(repository, hash);
        display.printTree(tree);
    }
}