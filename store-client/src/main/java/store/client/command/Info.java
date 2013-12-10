package store.client.command;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.ContentInfo;
import store.common.Hash;

class Info extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.HASH));

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Print info about an existing content in current volume";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String volume = session.getVolume();
        Hash hash = new Hash(params.get(0));
        ContentInfo info = session.getRestClient().info(volume, hash);
        display.print(asString(info));
    }
}
