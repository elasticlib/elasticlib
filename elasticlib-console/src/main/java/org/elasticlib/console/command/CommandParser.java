/* 
 * Copyright 2014 Guillaume Masclet <guillaume.masclet@yahoo.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticlib.console.command;

import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import static java.util.stream.Collectors.toCollection;
import javax.ws.rs.ProcessingException;
import jline.console.completer.Completer;
import org.elasticlib.common.exception.NodeException;
import org.elasticlib.console.config.ConsoleConfig;
import org.elasticlib.console.discovery.DiscoveryClient;
import org.elasticlib.console.display.Display;
import org.elasticlib.console.exception.RequestFailedException;
import org.elasticlib.console.http.Session;
import org.elasticlib.console.tokenizing.Tokenizing;
import static org.elasticlib.console.tokenizing.Tokenizing.argList;
import static org.elasticlib.console.tokenizing.Tokenizing.isComplete;

/**
 * Provide actual command implementations.
 */
public final class CommandParser implements Completer {

    private final Display display;
    private final Session session;
    private final ConsoleConfig config;
    private final ParametersCompleter parametersCompleter;

    /**
     * Constructor.
     *
     * @param display Display to inject.
     * @param session Session to inject.
     * @param config Config to inject.
     * @param discoveryClient Discovery client to inject.
     */
    public CommandParser(Display display, Session session, ConsoleConfig config, DiscoveryClient discoveryClient) {
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
        Set<String> completions = CommandProvider.commands()
                .stream()
                .map(command -> completion(command, argList))
                .filter(completion -> completion != null)
                .collect(toCollection(TreeSet::new));

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
