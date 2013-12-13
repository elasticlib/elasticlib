package store.client.command;

import static java.util.Arrays.asList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Drop extends AbstractCommand {

    private final Map<String, List<Type>> syntax = new LinkedHashMap<>();

    {
        syntax.put(VOLUME, asList(Type.VOLUME));
        syntax.put(INDEX, asList(Type.INDEX));
        syntax.put(REPLICATION, asList(Type.VOLUME, Type.VOLUME));
    }

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Drop an existing volume, index or replication";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        switch (params.get(0).toLowerCase()) {
            case VOLUME:
                session.getRestClient().dropVolume(params.get(1));
                break;

            case INDEX:
                session.getRestClient().dropIndex(params.get(1));
                break;

            case REPLICATION:
                session.getRestClient().dropReplication(params.get(1), params.get(2));
                break;

            default:
                throw new IllegalArgumentException(params.get(0));
        }
    }
}
