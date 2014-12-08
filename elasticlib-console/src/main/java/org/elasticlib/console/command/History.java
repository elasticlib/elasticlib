package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.common.model.Event;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

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
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        long cursor = Long.MAX_VALUE;
        List<Event> events;
        do {
            events = session.getRepository().history(false, cursor, CHUNK_SIZE);
            for (Event event : events) {
                cursor = event.getSeq() - 1;
                display.print(event);
            }
        } while (events.size() >= CHUNK_SIZE && cursor > 1);
    }
}
