package store.client;

import store.client.display.Display;
import store.client.http.Session;
import java.io.IOException;
import jline.console.ConsoleReader;
import store.client.command.CommandParser;
import store.client.exception.QuitException;

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
        Display display = new Display(consoleReader);
        try (Session session = new Session()) {
            CommandParser parser = new CommandParser(display, session);
            consoleReader.addCompleter(parser);
            consoleReader.setExpandEvents(false);
            String buffer;
            while ((buffer = consoleReader.readLine()) != null) {
                parser.execute(buffer);
            }
        } catch (QuitException e) {
            // It's ok, just leave cleanly.
        } finally {
            consoleReader.shutdown();
        }
    }
}
