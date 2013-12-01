package store.client;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import store.client.command.Command;
import static store.client.command.CommandFactory.command;

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
     */
    public static void main(String[] args) {
        try (Session session = new Session()) {
            String line;
            while ((line = session.getConsoleReader().readLine()) != null) {
                if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
                    break;
                }
                List<String> argList = Lists.newArrayList(Splitter.on(" ").trimResults().split(line));
                Optional<Command> OptCommand = command(argList);
                if (!OptCommand.isPresent()) {
                    session.out().println("Unsupported command !"); // TODO print help
                    session.out().flush();
                } else {
                    try {
                        Command command = OptCommand.get();
                        command.execute(session, argList);

                    } catch (RequestFailedException e) {
                        session.out().println(e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
