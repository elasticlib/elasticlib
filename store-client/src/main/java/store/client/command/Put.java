package store.client.command;

import java.nio.file.Path;
import java.nio.file.Paths;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.Map;
import store.client.Display;
import static store.client.FormatUtil.asString;
import store.client.Session;
import store.common.CommandResult;

class Put extends AbstractCommand {

    private final Map<String, List<Type>> syntax = singletonMap("", singletonList(Type.PATH));

    @Override
    protected Map<String, List<Type>> syntax() {
        return syntax;
    }

    @Override
    public String description() {
        return "Put a new content in current repository";
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        String repository = session.getRepository();
        Path path = Paths.get(params.get(0));
        CommandResult result = session.getRestClient().put(repository, path);
        display.print(asString(result));
    }
}
