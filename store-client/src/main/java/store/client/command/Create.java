package store.client.command;

import java.nio.file.Paths;
import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Create extends AbstractCommand {

    private final Map<String, List<Type>> syntax = new HashMap<>();

    {
        syntax.put(VOLUME, asList(Type.PATH));
        syntax.put(INDEX, asList(Type.PATH, Type.VOLUME));
        syntax.put(REPLICATION, asList(Type.VOLUME, Type.VOLUME));
    }

    @Override
    public Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        switch (params.get(0).toLowerCase()) {
            case VOLUME:
                session.getRestClient().createVolume(Paths.get(params.get(1)));
                break;

            case INDEX:
                session.getRestClient().createIndex(Paths.get(params.get(1)), params.get(2));
                break;

            case REPLICATION:
                session.getRestClient().createReplication(params.get(1), params.get(2));
                break;

            default:
                throw new IllegalArgumentException(params.get(0));
        }
    }
}
