package store.client.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import store.client.Display;
import store.client.Session;

class OsCommand implements Command {

    @Override
    public String name() {
        return "!";
    }

    @Override
    public String description() {
        return "Execute an OS specific command";
    }

    @Override
    public String usage() {
        return "Usage: !OS_COMMAND ARGS...";
    }

    @Override
    public List<String> complete(Session session, List<String> params) {
        return Collections.emptyList();
    }

    @Override
    public boolean isValid(List<String> params) {
        return !params.isEmpty();
    }

    @Override
    public void execute(Display display, Session session, List<String> params) {
        try {
            Process process = new ProcessBuilder(params).start();
            try (BufferedReader out = reader(process.getInputStream());
                    BufferedReader err = reader(process.getErrorStream())) {
                String buffer;
                while ((buffer = out.readLine()) != null) {
                    display.print(buffer);
                }
                while ((buffer = err.readLine()) != null) {
                    display.print(buffer);
                }
            } finally {
                process.destroy();
            }
        } catch (IOException e) {
            display.print(e.getMessage());
        }
    }

    private static BufferedReader reader(InputStream input) {
        return new BufferedReader(new InputStreamReader(input));
    }
}