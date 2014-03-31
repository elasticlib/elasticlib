package store.client.command;

import java.util.List;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.ContentInfo;

class Find extends AbstractCommand {

    Find() {
        super(Type.QUERY);
    }

    @Override
    public String description() {
        return "Search contents in current repository";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        String query = params.get(0);

        int cursor = 0;
        List<ContentInfo> infos;
        do {
            infos = session.getRestClient().findInfo(repository, query, cursor, 20);
            for (ContentInfo info : infos) {
                cursor += infos.size();
                display.print(asString(info));
            }
        } while (!infos.isEmpty());
    }
}
