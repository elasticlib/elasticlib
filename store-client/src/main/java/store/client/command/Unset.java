package store.client.command;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Unset extends AbstractCommand {

    private final Map<String, List<Type>> syntax = new LinkedHashMap<>();

    {
        syntax.put(VOLUME, Collections.<Type>emptyList());
        syntax.put(INDEX, Collections.<Type>emptyList());
    }

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Unset a session variable";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        switch (params.get(0).toLowerCase()) {
            case VOLUME:
                session.unsetVolume();
                display.resetPrompt();
                break;

            case INDEX:
                session.unsetIndex();
                break;

            default:
                throw new IllegalArgumentException(params.get(0));
        }
    }
}
