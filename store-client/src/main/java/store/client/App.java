package store.client;

import java.io.IOException;
import javax.ws.rs.ProcessingException;
import jline.console.ConsoleReader;
import store.client.command.CommandParser;
import store.client.config.ClientConfig;
import store.client.display.Display;
import store.client.exception.QuitException;
import store.client.exception.RequestFailedException;
import store.client.http.Session;
import store.client.util.EscapingCompletionHandler;

/**
 * Client starting.
 */
public final class App {

    private App() {
    }

    /**
     * Main method.
     *
     * @param args Command line arguments.
     * @throws IOException Unexpected.
     */
    public static void main(String[] args) throws IOException {
        ConsoleReader consoleReader = new ConsoleReader();
        ClientConfig config = new ClientConfig();
        Display display = new Display(consoleReader, config);
        try (Session session = new Session(display, config)) {
            try {
                config.init();
                display.println("Using config:" + System.lineSeparator() + config.print());
                session.init();

            } catch (ProcessingException | RequestFailedException e) {
                display.print(e);
            }
            CommandParser parser = new CommandParser(display, session, config);
            consoleReader.addCompleter(parser);
            consoleReader.setCompletionHandler(new EscapingCompletionHandler());
            consoleReader.setExpandEvents(false);

            String buffer = consoleReader.readLine();
            while (buffer != null) {
                parser.execute(buffer);
                buffer = consoleReader.readLine();
            }
        } catch (QuitException e) {
            // It's ok, just leave cleanly.
        } finally {
            consoleReader.shutdown();
        }
    }
}
