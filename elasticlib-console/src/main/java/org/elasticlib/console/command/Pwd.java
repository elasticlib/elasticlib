package org.elasticlib.console.command;

import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.Directories.workingDirectory;

class Pwd extends AbstractCommand {

    Pwd() {
        super(Category.MISC);
    }

    @Override
    public String description() {
        return "Print current working directory";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        display.println(workingDirectory().toString());
    }
}
