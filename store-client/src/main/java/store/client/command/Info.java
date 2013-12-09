package store.client.command;

import java.util.List;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.ContentInfo;
import store.common.Hash;

class Info extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        return completeImpl(session, args, Type.HASH);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        Hash hash = new Hash(args.get(1));
        ContentInfo info = session.getRestClient().info(volume, hash);
        display.print(asString(info));
    }
}
