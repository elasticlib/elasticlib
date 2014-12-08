package org.elasticlib.node;

import static java.lang.Thread.currentThread;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Node app.
 */
public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private App() {
    }

    /**
     * Main method.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            LOG.error("The path to this node home-directory has to be supplied in the command line arguments");
            return;
        }
        // Optionally remove existing handlers and add SLF4JBridgeHandler to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        Path home = Paths.get(args[0]);
        new Node(home).start();
        try {
            currentThread().join();

        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
