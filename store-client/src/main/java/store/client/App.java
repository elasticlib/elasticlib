package store.client;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import jline.console.ConsoleReader;
import store.client.command.Command;
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
            CommandParser parser = new CommandParser(session);
            consoleReader.addCompleter(parser);
            String line;
            while ((line = consoleReader.readLine()) != null) {
                List<String> argList = Lists.newArrayList(Splitter.on(" ").trimResults().split(line));
                Optional<Command> OptCommand = parser.command(argList);
                if (!OptCommand.isPresent()) {
                    display.print("Unsupported command !");  // TODO print help

                } else {
                    try {
                        Command command = OptCommand.get();
                        command.execute(display, session, argList);

                    } catch (RequestFailedException e) {
                        display.print(e.getMessage());
                    }
                }
            }
        }
    }
}
