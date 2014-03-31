package store.client;

import java.io.IOException;
import jline.console.ConsoleReader;
import store.client.command.CommandParser;
import store.client.display.Display;
import store.client.exception.QuitException;
import store.client.http.Session;

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
        try (Session session = new Session(display)) {
            CommandParser parser = new CommandParser(display, session);
            consoleReader.addCompleter(parser);
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
