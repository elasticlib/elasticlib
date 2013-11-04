package store.server;

import static java.lang.Thread.currentThread;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Server starting.
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
        if (args.length == 0) {
            System.out.println("Syntax : serve <home>");
            return;
        }
        // Optionally remove existing handlers and add SLF4JBridgeHandler to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        Path home = Paths.get(args[0]);
        new Server(home).start();
        try {
            currentThread().join();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
