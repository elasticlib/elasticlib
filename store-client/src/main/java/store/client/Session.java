package store.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import jline.console.ConsoleReader;

public class Session implements Closeable {

    private final RestClient restClient;
    private final ConsoleReader consoleReader;
    private final PrintWriter out;

    public Session() throws IOException {
        restClient = new RestClient();
        consoleReader = new ConsoleReader();
        consoleReader.setPrompt("$ ");
        out = new PrintWriter(consoleReader.getOutput());
    }

    public void setVolume(String name) {
        restClient.setVolume(name);
        consoleReader.setPrompt(name + "$ ");
    }

    public RestClient getRestClient() {
        return restClient;
    }

    public ConsoleReader getConsoleReader() {
        return consoleReader;
    }

    public PrintWriter out() {
        return out;
    }

    @Override
    public void close() {
        restClient.close();
    }
}
