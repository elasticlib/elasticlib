package store.client.command;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.ws.rs.ProcessingException;
import jline.console.completer.Completer;
import store.client.config.ClientConfig;
import store.client.discovery.DiscoveryClient;
import store.client.display.Display;
import store.client.http.Session;
import store.client.tokenizing.Tokenizing;
import static store.client.tokenizing.Tokenizing.argList;
import static store.client.tokenizing.Tokenizing.isComplete;
import store.common.client.RequestFailedException;
import store.common.exception.NodeException;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private final Display display;
    private final Session session;
    private final ClientConfig config;
    private final ParametersCompleter parametersCompleter;

    /**
     * Constructor.
     *
     * @param display Display to inject.
     * @param session Session to inject.
     * @param config Config to inject.
     * @param discoveryClient Discovery client to inject.
     */
    public CommandParser(Display display, Session session, ClientConfig config, DiscoveryClient discoveryClient) {
        this.display = display;
        this.session = session;
        this.config = config;
        parametersCompleter = new ParametersCompleter(session, discoveryClient);
    }

    /**
     * Parse and execute supplied command line buffer.
     *
     * @param buffer Command line buffer.
     */
    public void execute(String buffer) {
        List<String> argList = argList(buffer);
        if (argList.isEmpty()) {
            return;
        }
        Optional<Command> commandOpt = CommandProvider.command(argList);
        if (!commandOpt.isPresent()) {
            display.println(CommandProvider.help());
            return;
        }
        Command command = commandOpt.get();
        List<String> params = command.params(argList);
        if (!command.isValid(params)) {
            display.println(command.usage() + System.lineSeparator());
            return;
        }
        execute(command, params);
    }

    private void execute(Command command, List<String> params) {
        try {
            session.printHttpDialog(true);
            command.execute(display, session, config, params);

        } catch (NodeException e) {
            display.print(e);

        } catch (RequestFailedException e) {
            display.print(e);
            session.disconnect();

        } catch (ProcessingException e) {
            display.print(e);
            session.disconnect();

        } finally {
            session.printHttpDialog(false);
        }
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        if (buffer.length() != cursor) {
            return 0;
        }
        List<String> argList = argList(buffer);
        boolean emptyArg = buffer.isEmpty() || (isComplete(buffer) && buffer.endsWith(" "));
        if (emptyArg) {
            argList = new ArrayList<>(argList);
            argList.add("");
        }
        List<String> completions = completions(argList);
        candidates.addAll(completions);
        return emptyArg ? buffer.length() : Tokenizing.lastArgumentPosition(buffer);
    }

    private List<String> completions(List<String> argList) {
        Optional<Command> commandOpt = CommandProvider.command(argList);
        if (commandOpt.isPresent()) {
            Command command = commandOpt.get();
            List<String> params = command.params(argList);
            if (!params.isEmpty()) {
                return command.complete(parametersCompleter, params);
            }
        }
        Set<String> completions = new TreeSet<>();
        for (Command command : CommandProvider.commands()) {
            String completion = completion(command, argList);
            if (completion != null) {
                completions.add(completion);
            }
        }
        return new ArrayList<>(completions);
    }

    private static String completion(Command command, List<String> argList) {
        Iterator<String> parts = Splitter.on(' ').split(command.name()).iterator();
        Iterator<String> args = argList.iterator();
        while (parts.hasNext()) {
            String part = parts.next();
            String arg = args.hasNext() ? args.next() : "";
            if (!part.startsWith(arg)) {
                return null;
            }
            if (!args.hasNext()) {
                return part;
            }
        }
        return null;
    }
}
