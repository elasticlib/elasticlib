package store.client.command;

import com.google.common.base.Joiner;
import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Volumes extends AbstractCommand {

    @Override
    public Map<String, List<Type>> syntax() {
        return emptyMap();
    }

    @Override
    public void execute(Display display, Session session, List<String> args) {
        List<String> volumes = session.getRestClient().listVolumes();
        display.print(Joiner.on(System.lineSeparator()).join(volumes));
    }
}
