package store.client.command;

import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Set extends AbstractCommand {

    private final Map<String, List<Type>> syntax = new HashMap<>();

    {
        syntax.put(VOLUME, asList(Type.VOLUME));
        syntax.put(INDEX, asList(Type.INDEX));
    }

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        switch (args.get(1).toLowerCase()) {
            case VOLUME:
                String volume = args.get(2);
                session.setVolume(volume);
                display.setPrompt(volume);
                break;

            case INDEX:
                session.setIndex(args.get(2));
                break;

            default:
                throw new IllegalArgumentException(args.get(1));
        }
    }
}
