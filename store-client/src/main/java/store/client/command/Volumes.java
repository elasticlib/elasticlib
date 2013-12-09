package store.client.command;

import com.google.common.base.Joiner;
import java.util.List;
import store.client.Display;
import store.client.Session;

class Volumes extends AbstractCommand {

    @Override
    public List<String> complete(Session session, List<String> args) {
        return completeImpl(session, args);
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        List<String> volumes = session.getRestClient().listVolumes();
        display.print(Joiner.on(System.lineSeparator()).join(volumes));
    }
}
