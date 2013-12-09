package store.client.command;

import java.nio.file.Paths;
import static java.util.Arrays.asList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Create extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        Map<String, List<Type>> syntax = new HashMap<>();
        syntax.put(VOLUME, asList(Type.PATH));
        syntax.put(INDEX, asList(Type.PATH, Type.VOLUME));
        syntax.put(REPLICATION, asList(Type.VOLUME, Type.VOLUME));

        return completeImpl(session, args, syntax);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        switch (args.get(1).toLowerCase()) {
            case VOLUME:
                session.getRestClient().createVolume(Paths.get(args.get(2)));
                break;

            case INDEX:
                session.getRestClient().createIndex(Paths.get(args.get(2)), args.get(3));
                break;

            case REPLICATION:
                session.getRestClient().createReplication(args.get(2), args.get(3));
                break;

            default:
                throw new IllegalArgumentException(args.get(1));
        }
    }
}
