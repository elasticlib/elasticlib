package store.client;

import java.io.IOException;
import jline.console.ConsoleReader;
import store.client.command.CommandParser;

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
            String buffer;
            while ((buffer = consoleReader.readLine()) != null) {
                parser.execute(buffer);
            }
        }
    }
}
