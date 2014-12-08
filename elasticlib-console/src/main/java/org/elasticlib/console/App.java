package org.elasticlib.console;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import javax.ws.rs.ProcessingException;
import jline.console.ConsoleReader;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.console.command.CommandParser;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.discovery.DiscoveryClient;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.QuitException;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import org.elasticlib.console.util.EscapingCompletionHandler;

/**
 * Console app.
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
        ConsoleConfig config = new ConsoleConfig();
        Display display = new Display(consoleReader, config);
        DiscoveryClient discoveryClient = new DiscoveryClient(config);

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

            } catch (NodeException e) {
                display.print(e);
            } catch (RequestFailedException e) {
                display.print(e);
            } catch (ProcessingException e) {
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
