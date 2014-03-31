package store.client.command;

import java.util.List;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.ContentInfoTree;
import store.common.Hash;

class Info extends AbstractCommand {

    Info() {
        super(Type.HASH);
    }

    @Override
    public String description() {
        return "Print info about an existing content in current repository";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        Hash hash = new Hash(params.get(0));
        ContentInfoTree tree = session.getRestClient().getInfoTree(repository, hash);
        display.print(asString(tree));
    }
}
