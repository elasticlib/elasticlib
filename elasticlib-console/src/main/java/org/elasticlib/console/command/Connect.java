package org.elasticlib.console.command;

import java.net.URI;
import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.parseUri;

class Connect extends AbstractCommand {

    Connect() {
        super(Category.NODE, Type.URI);
    }

    @Override
    public String description() {
        return "Connect to a node";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        URI uri = parseUri(params.get(0));
        session.connect(uri);
        display.printOk();
    }
}
