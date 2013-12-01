package store.client.command;

import java.nio.file.Paths;
import java.util.Collections;
import static java.util.Collections.singletonList;
import java.util.List;
import store.client.Session;
import store.client.Type;
import static store.client.Type.PATH;

class CreateVolume extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Collections.emptyList();
    }

    @Override
    public List<Type> args() {
        return singletonList(PATH);
    }

    @Override
    public void execute(Session session, List<String> args) {
        session.getRestClient().createVolume(Paths.get(args.get(2)));
    }
}
