package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.model.ContentInfo;

class Find extends AbstractCommand {

    private static final int CHUNK_SIZE = 20;

    Find() {
        super(Category.CONTENTS, Type.QUERY);
    }

    @Override
    public String description() {
        return "Search contents";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        int cursor = 0;
        List<ContentInfo> infos;
        do {
            infos = session.getRepository().findInfo(params.get(0), cursor, CHUNK_SIZE);
            for (ContentInfo info : infos) {
                cursor += infos.size();
                display.print(info);
            }
        } while (infos.size() >= CHUNK_SIZE);
    }
}
