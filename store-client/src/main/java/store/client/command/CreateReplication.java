package store.client.command;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import store.client.Session;
import store.client.Type;
import static store.client.Type.VOLUME;

class CreateReplication extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Collections.emptyList();
    }

    @Override
    public List<Type> args() {
        return Arrays.asList(VOLUME, VOLUME);
    }

    @Override
    public void execute(Session session, List<String> args) {
        session.getRestClient().createReplication(args.get(2), args.get(3));
    }
}
