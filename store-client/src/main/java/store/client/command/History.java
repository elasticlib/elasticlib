package store.client.command;

import static java.util.Collections.singletonList;
import java.util.List;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.client.Type;
import static store.client.Type.HASH;
import static store.client.Type.VOLUME;
import store.common.Event;

class History extends AbstractCommand {

    @Override
    public List<Type> env() {
        return singletonList(VOLUME);
    }

    @Override
    public List<Type> args() {
        return singletonList(HASH);
    }

    @Override
    public void execute(Session session, List<String> args) {
        String volume = session.getVolume();
        long cursor = Long.MAX_VALUE;
        List<Event> events;
        do {
            events = session.getRestClient().history(volume, false, cursor, 20);
            for (Event event : events) {
                cursor = event.getSeq();
                session.out().println(asString(event));
            }
        } while (!events.isEmpty() && cursor > 1);
    }
}
