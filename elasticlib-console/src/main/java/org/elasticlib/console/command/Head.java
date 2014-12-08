package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.ClientUtil.parseHash;

class Head extends AbstractCommand {

    Head() {
        super(Category.CONTENTS, Type.HASH);
    }

    @Override
    public String description() {
        return "Print head revisions of an existing content";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        Hash hash = parseHash(params.get(0));
        session.getRepository()
                .getHead(hash)
                .forEach(display::print);
    }
}
