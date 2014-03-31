package store.client.command;

import java.util.List;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.Event;

class History extends AbstractCommand {

    @Override
    public String description() {
        return "Print current repository history";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        long cursor = Long.MAX_VALUE;
        List<Event> events;
        do {
            events = session.getRestClient().history(repository, false, cursor, 20);
            for (Event event : events) {
                cursor = event.getSeq();
                display.print(asString(event));
            }
        } while (!events.isEmpty() && cursor > 1);
    }
}
