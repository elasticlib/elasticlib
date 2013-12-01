package store.client.command;

import java.util.Collections;
import java.util.List;
import store.client.Session;
import store.client.Type;

class UnsetIndex extends AbstractCommand {

    @Override
    public List<Type> env() {
        return Collections.emptyList();
    }

    @Override
    public List<Type> args() {
        return Collections.emptyList();
    }

    @Override
    public void execute(Session session, List<String> args) {
        session.unsetIndex();
    }
}
