package store.client.command;

import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.Event;

class History extends AbstractCommand {

    @Override
    public Map<String, List<Type>> syntax() {
        return emptyMap();
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        String volume = session.getVolume();
        long cursor = Long.MAX_VALUE;
        List<Event> events;
        do {
            events = session.getRestClient().history(volume, false, cursor, 20);
            for (Event event : events) {
                cursor = event.getSeq();
                display.print(asString(event));
            }
        } while (!events.isEmpty() && cursor > 1);
    }
}
