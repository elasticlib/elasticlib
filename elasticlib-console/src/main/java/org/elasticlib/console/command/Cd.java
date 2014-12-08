package org.elasticlib.console.command;

import java.nio.file.Paths;
import java.util.List;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.http.Session;
import static org.elasticlib.console.util.Directories.changeToUserHome;
import static org.elasticlib.console.util.Directories.changeWorkingDirectory;

class Cd extends AbstractCommand {

    Cd() {
        super(Category.MISC, Type.DIRECTORY);
    }

    @Override
    public String description() {
        return "Change current working directory";
    }

    @Override
    public void execute(Display display, Session session, ConsoleConfig config, List<String> params) {
        if (params.get(0).equals("~")) {
            changeToUserHome();
            return;
        }
        changeWorkingDirectory(Paths.get(params.get(0)));
    }
}
