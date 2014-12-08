package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.common.model.NodeDef;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;

class Node extends AbstractCommand {

    Node() {
        super(Category.NODE);
    }

    @Override
    public String description() {
        return "Display info about current node";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        NodeDef def = session.getClient()
                .node()
                .getDef();

        display.print(def);
    }
}
