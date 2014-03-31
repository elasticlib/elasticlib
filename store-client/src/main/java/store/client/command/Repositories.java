package store.client.command;

import com.google.common.base.Joiner;
import java.util.List;
import store.client.Display;
import store.client.Session;

class Repositories extends AbstractCommand {

    @Override
    public String description() {
        return "List existing repositories";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        List<String> repositories = session.getRestClient().listRepositories();
        display.print(Joiner.on(System.lineSeparator()).join(repositories));
    }
}
