package store.client;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import javax.ws.rs.ProcessingException;
import jline.console.ConsoleReader;
import store.client.command.CommandParser;
import store.client.config.ClientConfig;
import store.client.discovery.DiscoveryClient;
import store.client.display.Display;
import store.client.exception.QuitException;
import store.client.http.Session;
import store.client.util.EscapingCompletionHandler;
import store.common.client.RequestFailedException;

/**
 * Client app.
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
        final DiscoveryClient discoveryClient = new DiscoveryClient(config);

        getRuntime().addShutdownHook(new Thread("shutdown") {
            @Override
            public void run() {
                // Do not shutdown the consoleReader here, it deadlocks otherwise.
                discoveryClient.stop();
            }
        });

        try (Session session = new Session(display, config)) {
            try {
                config.init();
                display.println("Using config:" + System.lineSeparator() + config.print());
                session.init();

            } catch (ProcessingException | RequestFailedException e) {
                display.print(e);
            }

            discoveryClient.start();

            CommandParser parser = new CommandParser(display, session, config, discoveryClient);
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
