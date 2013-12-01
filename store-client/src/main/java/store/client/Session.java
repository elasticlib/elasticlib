package store.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import jline.console.ConsoleReader;

/**
 * A command line session. Keep everything that need to survive across sussessive command invocations.
 */
public class Session implements Closeable {

    private static final String PROMPT = "> ";
    private final RestClient restClient;
    private final ConsoleReader consoleReader;
    private final PrintWriter out;
    private String volume;
    private String index;

    public Session() throws IOException {
        restClient = new RestClient();
        consoleReader = new ConsoleReader();
        consoleReader.setPrompt(PROMPT);
        out = new PrintWriter(consoleReader.getOutput());
    }

    public void setVolume(String volume) {
        this.volume = volume;
        consoleReader.setPrompt(volume + PROMPT);
    }

    public void unsetVolume() {
        volume = null;
        consoleReader.setPrompt(PROMPT);
    }

    public String getVolume() {
        return volume;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void unsetIndex() {
        index = null;
    }

    public String getIndex() {
        return index;
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
