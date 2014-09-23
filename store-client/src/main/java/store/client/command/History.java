package store.client.command;

import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.common.model.Event;

class History extends AbstractCommand {

    private static final int CHUNK_SIZE = 20;

    History() {
        super(Category.CONTENTS);
    }

    @Override
    public String description() {
        return "Print current repository history";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        String repository = session.getRepository();
        long cursor = Long.MAX_VALUE;
        List<Event> events;
        do {
            events = session.getClient().history(repository, false, cursor, CHUNK_SIZE);
            for (Event event : events) {
                cursor = event.getSeq();
                display.print(event);
            }
        } while (events.size() >= CHUNK_SIZE && cursor > 1);
    }
}
