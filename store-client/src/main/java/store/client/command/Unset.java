package store.client.command;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Unset extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        Map<String, List<Type>> syntax = new HashMap<>();
        syntax.put(VOLUME, Collections.<Type>emptyList());
        syntax.put(INDEX, Collections.<Type>emptyList());

        return completeImpl(session, args, syntax);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        switch (args.get(1).toUpperCase()) {
            case VOLUME:
                session.unsetVolume();
                display.resetPrompt();
                break;

            case INDEX:
                session.unsetIndex();
                break;

            default:
                throw new IllegalArgumentException(args.get(1));
        }
    }
}
