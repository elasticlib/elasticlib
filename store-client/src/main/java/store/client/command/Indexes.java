package store.client.command;

import com.google.common.base.Joiner;
import static java.util.Collections.emptyMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import store.client.Session;

class Indexes extends AbstractCommand {

    @Override
    public Map<String, List<Type>> syntax() {
        return emptyMap();
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        List<String> volumes = session.getRestClient().listIndexes();
        display.print(Joiner.on(System.lineSeparator()).join(volumes));
    }
}
