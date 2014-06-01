package store.client.command;

import java.nio.file.Paths;
import static java.util.Arrays.asList;
import java.util.List;
import static store.client.command.AbstractCommand.REPOSITORY;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.http.Session;
import store.client.util.Directories;

class Add extends AbstractCommand {

    Add() {
        super(Category.SERVER, REPOSITORY, asList(Type.PATH));
    }

    @Override
    public String description() {
        return "Add an existing repository to this server";
    }

    @Override
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        session.getClient().addRepository(Directories.resolve(Paths.get(params.get(1))));
        display.println(OK);
    }
}
