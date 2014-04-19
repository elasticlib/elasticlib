package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
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
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        String query = params.get(0);

        int cursor = 0;
        List<ContentInfo> infos;
        do {
            infos = session.getClient().findInfo(repository, query, cursor, 20);
            for (ContentInfo info : infos) {
                cursor += infos.size();
                display.print(info);
            }
        } while (!infos.isEmpty());
    }
}
