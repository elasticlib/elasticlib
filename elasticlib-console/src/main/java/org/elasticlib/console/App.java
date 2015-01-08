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
package org.elasticlib.console;

import java.io.IOException;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.lineSeparator;
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
import static org.elasticlib.console.util.Directories.workingDirectory;
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
                display.println(about());
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

            display.setPrompt(session.getConnectionString(), workingDirectory());
            String buffer = consoleReader.readLine();
            while (buffer != null) {
                parser.execute(buffer);
                display.setPrompt(session.getConnectionString(), workingDirectory());
                buffer = consoleReader.readLine();
            }
        } catch (QuitException e) {
            // It's ok, just leave cleanly.
        } finally {
            consoleReader.shutdown();
        }
    }

    private static String about() {
        Package pkg = App.class.getPackage();
        String title = pkg.getImplementationTitle() == null ? "" : pkg.getImplementationTitle();
        String version = pkg.getImplementationVersion() == null ? "" : pkg.getImplementationVersion();
        return String.join("", title, " ", version, lineSeparator());
    }
}
