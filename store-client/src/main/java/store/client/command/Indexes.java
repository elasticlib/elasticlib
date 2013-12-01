package store.client.command;

import com.google.common.base.Joiner;
import java.util.Collections;
import java.util.List;
import store.client.Session;
import store.client.Type;

class Indexes extends AbstractCommand {

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
        List<String> volumes = session.getRestClient().listIndexes();
        session.out().println(Joiner.on(System.lineSeparator()).join(volumes));
    }
}
