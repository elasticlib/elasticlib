package store.client.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.file.Directories;
import store.client.http.Session;

class OsCommand implements Command {

    @Override
    public String name() {
        return "!";
    }

    @Override
    public Category category() {
        return Category.MISC;
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
    public void execute(Display display, Session session, ClientConfig config, List<String> params) {
        try {
            Process process = new ProcessBuilder(params)
                    .directory(Directories.workingDirectory().toFile())
                    .start();

            try (BufferedReader out = reader(process.getInputStream());
                    BufferedReader err = reader(process.getErrorStream())) {

                String buffer = out.readLine();
                while (buffer != null) {
                    display.println(buffer);
                    buffer = out.readLine();
                }
                buffer = err.readLine();
                while (buffer != null) {
                    display.println(buffer);
                    buffer = err.readLine();
                }
            } finally {
                process.destroy();
            }
        } catch (IOException e) {
            display.println(e.getMessage());
        }
    }

    private static BufferedReader reader(InputStream input) {
        return new BufferedReader(new InputStreamReader(input));
    }
}
