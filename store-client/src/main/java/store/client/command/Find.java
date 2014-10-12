package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.model.Revision;

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
        List<Revision> revisions;
        do {
            revisions = session.getRepository().findRevisions(params.get(0), cursor, CHUNK_SIZE);
            for (Revision rev : revisions) {
                cursor += revisions.size();
                display.print(rev);
            }
        } while (revisions.size() >= CHUNK_SIZE);
    }
}
